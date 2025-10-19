#!/usr/bin/env python3
"""
Merge QS, THE, and US News university ranking datasets.

This script combines data from three global university ranking sources:
- QS 2026 World University Rankings
- THE 2025 World University Rankings
- US News Latin America & Caribbean Rankings (Liberal Arts Colleges)

Features:
- Remove China-related institutions (China, Taiwan, Hong Kong, Macau)
- Normalize university names and countries
- Merge rankings and coordinates from all sources
- Normalize institution type to Public/Private
"""

import pandas as pd
import re
from pathlib import Path
from difflib import SequenceMatcher
import warnings
from tqdm import tqdm

# Suppress FutureWarning for pd.concat
warnings.filterwarnings("ignore", category=FutureWarning)

# =============== 参数配置 ===============
# Note: FUZZY_THRESHOLD and STOPWORDS removed - using strict exact-match deduplication only

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
SCRIPT_DIR = Path(__file__).parent  # 脚本所在目录 (processors)

# Input files
QS_FILE = OUTPUT_DIR / "QS_2026_Rankings_with_Coordinates.csv"
THE_FILE = OUTPUT_DIR / "THE_2025_Rankings.csv"
USNEWS_FILE = OUTPUT_DIR / "USnews_LAC_Rankings.csv"

# Log file - 保存到脚本同级目录
MERGE_LOG_FILE = SCRIPT_DIR / "merge.log"

# Global list to accumulate log messages
process_log = []


def log_message(message, level="INFO"):
    """
    记录消息到全局日志列表和标准输出。

    Args:
        message: 要记录的消息
        level: 日志级别 (INFO, MERGE, DEDUP, SKIP, ERROR)
    """
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level:6s}] {message}"
    process_log.append(log_entry)

    # 打印到控制台 (使用 ASCII 符号避免编码问题)
    if level == "ERROR":
        print(f"  [ERROR] {message}")
    elif level == "MERGE":
        print(f"  [MERGE] {message}")
    elif level == "DEDUP":
        print(f"  [DEDUP] {message}")
    elif level == "SKIP":
        print(f"  [SKIP]  {message}")
    else:
        print(f"  [INFO] {message}")


# Output file
MERGED_FILE = OUTPUT_DIR / "merged_rankings.csv"

# Countries/regions to exclude (China-related)
EXCLUDE_COUNTRIES = {
    "China (Mainland)",
    "China",
    "Taiwan",
    "Hong Kong SAR, China",
    "Hong Kong",
    "Macao",
    "Macau",
    "Macao SAR, China",
}


def normalize_name(name):
    """
    温和的名称标准化（仅处理大小写和空格）：
    - 去除首尾空格
    - 大小写统一为小写
    - 忽略多余空格
    - 忽略逗号后的空格（如 "University of California, Berkeley" = "University of California Berkeley"）
    - 移除前缀 "the " （如 "The University of Georgia" → "University of Georgia"）
    """
    if pd.isna(name) or not name:
        return ""

    s = str(name).strip()
    # 统一大小写
    s = s.lower()
    # 移除逗号，用空格替代
    s = s.replace(",", " ")
    # 归一化多余空格
    s = re.sub(r"\s+", " ", s).strip()
    # 移除前缀 "the "
    s = re.sub(r"^the\s+", "", s)
    return s


def normalize_country(country):
    """
    Normalize country name.
    """
    if pd.isna(country):
        return ""

    country = str(country).strip()

    # Country normalization mappings
    country_map = {
        "United States": "United States of America",
        "USA": "United States of America",
        "US": "United States of America",
        "UK": "United Kingdom",
        "South Korea": "Republic of Korea",
        "North Korea": "Democratic People's Republic of Korea",
        "Netherlands": "Netherlands",
        "Holland": "Netherlands",
        "Macao": "Macau",
    }

    country = country_map.get(country, country)
    return country.strip()


def normalize_nature_of_running(status):
    """
    Normalize institution type to Public or Private.
    """
    if pd.isna(status):
        return None

    status = str(status).strip().lower()

    # Check for private indicators
    if any(
        term in status
        for term in ["private", "not for profit", "independent", "proprietary"]
    ):
        return "Private"

    # Check for public indicators
    if any(term in status for term in ["public", "government", "state", "federal"]):
        return "Public"

    # Default: if unclear, return None
    return None


def extract_base_and_alias(name: str):
    """
    提取主名和括号内别名。

    过滤掉明显不是校名别名的内容（国家代码、主校区标识等）。
    如 'Massachusetts Institute of Technology (MIT)' → ('massachusetts institute of technology', 'mit')
    但 'University of South Alabama (USA)' → ('university of south alabama', '') [USA被过滤]
    """
    if not isinstance(name, str):
        return "", ""

    name_original = name.strip()
    name = name_original.lower()
    match = re.match(r"^(.*?)\s*\((.*?)\)\s*$", name)
    if match:
        base = match.group(1).strip()
        alias = match.group(2).strip()

        # 过滤掉明显不是校名的别名
        # 国家代码
        ignore_patterns = {
            "usa",
            "uk",
            "u.k.",
            "us",
            "u.s.",
            "china",
            "prc",
            "canada",
            "australia",
            "nz",
            "n.z.",
            "singapore",
            "sg",
            "hong kong",
            "hk",
            "taiwan",
            "japan",
            "jp",
            "south korea",
            "korea",
            "india",
            "in",
            "germany",
            "de",
            "france",
            "fr",
            # 主校区/分校标识
            "main campus",
            "main campus.",
            "main",
            "branch",
            "branch campus",
            "campus",
            "state",
            "city",
            "center",
            "centre",
            # 其他非校名内容
            "formerly",
            "f.k.a",
            "f.k.a.",
            "aka",
            "previously",
            "merged from",
        }

        # 检查完整alias是否在过滤列表中
        if alias.lower() in ignore_patterns:
            alias = ""
        # 检查alias是否以过滤词开头
        elif any(alias.lower().startswith(pattern) for pattern in ignore_patterns):
            alias = ""

        return base, alias
    return name, ""


def normalize_text(text: str) -> str:
    """基础清洗（去符号、统一空格）"""
    if pd.isna(text):
        return ""
    text = re.sub(r"[,\.\-–—]+", " ", text.lower())
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_name_keys(name: str):
    """
    为每个学校生成一组匹配键，用于识别同一所大学。

    重要过滤规则：
    - 忽略国家代码（USA, UK, China等）作为别名
    - 忽略主校区标识（Main Campus等）

    例如 'Massachusetts Institute of Technology (MIT)' 会生成：
    - 'massachusetts institute of technology (mit)'
    - 'massachusetts institute of technology'
    - 'mit' + 展开后的 'massachusetts institute of technology'

    但 'University of South Alabama (USA)' 会生成：
    - 'university of south alabama'
    - 不会添加 'usa' 因为它被过滤掉了
    """
    base, alias = extract_base_and_alias(name)
    base_norm = normalize_name(base)
    alias_norm = normalize_name(alias) if alias else ""
    name_norm = normalize_name(name)

    keys = set()

    # 添加完整名称（可能包含括号）
    if name_norm:
        keys.add(name_norm)

    # 添加去掉括号后的主名
    if base_norm:
        keys.add(base_norm)

    # 添加括号内的别名（经过过滤）
    if alias_norm and len(alias_norm) > 0:
        # 二次过滤：检查别名是否太短或是否为明显的非校名内容
        if len(alias_norm) > 2:  # 别名必须至少 3 个字符
            # 再次检查是否是国家代码或其他无关内容
            disallowed_short = {
                "usa",
                "uk",
                "us",
                "prc",
                "nz",
                "sg",
                "hk",
                "de",
                "fr",
                "jp",
                "in",
            }
            if alias_norm.lower() not in disallowed_short:
                keys.add(alias_norm)

        # 常见缩写展开映射（仅用于有效的校名缩写）
        abbreviations = {
            "mit": "massachusetts institute of technology",
            "caltech": "california institute of technology",
            "eth zurich": "swiss federal institute of technology zurich",
            "nus": "national university of singapore",
            "ntu singapore": "nanyang technological university singapore",
            "ucl": "university college london",
            "uc berkeley": "university of california berkeley",
            "ucla": "university of california los angeles",
            "usc": "university of southern california",
            "ucsd": "university of california san diego",
            "nyu": "new york university",
            "uc": "university of california",
            "lse": "london school of economics",
            "eth": "swiss federal institute of technology",
        }

        if alias_norm in abbreviations:
            keys.add(abbreviations[alias_norm])

    return keys


def is_same_university(row_a, row_b):
    """
    判断两行是否表示同一所大学。

    规则：
    1. 国家必须相同
    2. 名称键集合必须有交集（说明是同一所大学的不同表达形式）
    3. 额外安全检查：防止因为通用词造成的误合并
    """
    # 比较国家（大小写不敏感）
    country_a = (
        str(row_a.get("Country", "")).strip().lower() if row_a.get("Country") else ""
    )
    country_b = (
        str(row_b.get("Country", "")).strip().lower() if row_b.get("Country") else ""
    )

    if not country_a or not country_b or country_a != country_b:
        return False

    # 比较名称键
    keys_a = row_a.get("_name_keys", set())
    keys_b = row_b.get("_name_keys", set())

    intersection = keys_a.intersection(keys_b)
    if len(intersection) == 0:
        return False

    # 额外安全检查：防止仅因为包含通用词而合并
    # 如果交集中只有"university"或"college"这样的通用词，则不合并
    common_words = {
        "university",
        "college",
        "institute",
        "school",
        "academy",
        "center",
        "centre",
    }

    # 检查交集中是否存在实质性内容（不仅仅是通用词）
    has_substantial_match = False
    for key in intersection:
        # 如果交集元素不仅包含通用词，就有实质性匹配
        words = set(key.split())
        if not words.issubset(common_words):
            has_substantial_match = True
            break

    return has_substantial_match


def is_china_related(country):
    """
    Check if country is China-related.
    """
    if pd.isna(country):
        return False

    country = str(country).strip()
    return country in EXCLUDE_COUNTRIES


def string_similarity(a, b):
    """
    Calculate string similarity ratio (0-1).
    """
    return SequenceMatcher(None, a, b).ratio()


def merge_fields(*values):
    """
    从多个值中选择第一个非空值。
    用于合并同一学校的不同记录中的字段数据。
    """
    for v in values:
        if pd.notna(v) and v != "" and str(v).strip() != "":
            return v
    return None


def exact_name_match(name1: str, name2: str) -> bool:
    """
    严格的名称匹配：规范化后完全相同。
    规范化规则：
    - 大小写不敏感
    - 忽略多余空格
    - 忽略逗号和逗号后的空格
    """
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    return n1 == n2 and n1 != ""


def deduplicate_fuzzy(df):
    """
    智能去重算法 v3 - 支持括号别名匹配（如 A(B) ↔ A 或 A(B) ↔ B）

    规则：
    1. 国家必须相同
    2. 名称键集合有交集 → 认定为同一所学校
    3. 从同一所学校的多条记录中，选择字段最完整的记录
    4. 尝试从其他记录填补空值
    5. 详细记录所有合并决策
    """
    print("\nDeduplicating with smart bracket-aware matching...")
    log_message("Starting intelligent deduplication with bracket matching")

    df = df.reset_index(drop=True)
    n = len(df)

    # 为每条记录预先计算名称键
    df["_name_keys"] = df["Name"].apply(build_name_keys)

    # 追踪已处理的记录
    used_indices = set()
    merged_groups = []
    merge_details = []

    for i, row_i in df.iterrows():
        if i in used_indices:
            continue

        # 开始一个新的group
        group = [i]
        used_indices.add(i)

        # 查找与row_i相同的其他记录
        for j in range(i + 1, len(df)):
            if j in used_indices:
                continue

            row_j = df.iloc[j]

            if is_same_university(row_i, row_j):
                group.append(j)
                used_indices.add(j)

        merged_groups.append(group)

    # 从每个group中选择代表记录
    keep_rows = []
    merge_count = 0

    for group_indices in merged_groups:
        if len(group_indices) == 1:
            # 单条记录，直接保留
            idx = group_indices[0]
            keep_rows.append(df.iloc[idx])
            continue

        # 多条记录需要合并
        merge_count += 1

        # 为这个group中的每条记录计算完整度分数
        group_with_scores = []
        for idx in group_indices:
            row = df.iloc[idx]
            score = row.notna().sum()  # 非空字段数
            group_with_scores.append((idx, row, score))

        # 按完整度排序，选择最完整的
        group_with_scores.sort(key=lambda x: x[2], reverse=True)

        representative_idx, representative, _ = group_with_scores[0]
        representative = representative.copy()

        # 记录合并信息
        country = representative.get("Country", "Unknown")
        orig_names = " | ".join([df.iloc[idx]["Name"] for idx in group_indices])
        merge_details.append(f"DEDUP: [{country}] {orig_names}")
        merge_details.append(
            f"  → Keep record #{representative_idx}: {representative['Name']}"
        )

        # 尝试从其他记录填补空值
        filled_fields = []
        for col in representative.index:
            if col.startswith("_") or col in ["Name", "Country"]:
                continue

            if pd.isna(representative[col]) or representative[col] == "":
                # 从其他记录查找该字段的值
                for alt_idx, alt_row, _ in group_with_scores[1:]:
                    alt_val = alt_row.get(col)
                    if pd.notna(alt_val) and str(alt_val).strip() != "":
                        representative[col] = alt_val
                        filled_fields.append(f"{col} from #{alt_idx}")
                        break

        if filled_fields:
            merge_details.append(f"  → Filled: {', '.join(filled_fields)}")

        keep_rows.append(representative)

    result = pd.DataFrame(keep_rows).reset_index(drop=True)

    # 删除临时列
    cols_to_drop = [c for c in result.columns if c.startswith("_")]
    result = result.drop(columns=cols_to_drop)

    # 记录统计信息
    deduped_count = n - len(result)
    preservation_rate = (len(result) / n * 100) if n > 0 else 0

    log_message(
        f"Deduplication complete: {len(result)} universities (merged {deduped_count} duplicates)",
        "DEDUP",
    )
    log_message(f"Preservation rate: {preservation_rate:.1f}%")

    # 保存详细的merge日志到脚本同级目录
    with open(MERGE_LOG_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        f.write("Deduplication Merge Log\n")
        f.write("=" * 90 + "\n\n")
        f.write(f"Total input records: {n}\n")
        f.write(f"Total merged groups: {merge_count}\n")
        f.write(f"Total duplicates merged: {deduped_count}\n")
        f.write(f"Final unique universities: {len(result)}\n")
        f.write(f"Preservation rate: {preservation_rate:.1f}%\n")
        f.write("\n" + "=" * 90 + "\n")
        f.write("Detailed Merge Operations:\n")
        f.write("=" * 90 + "\n\n")
        f.write("\n".join(merge_details))

    log_message(f"Merge log saved to: {MERGE_LOG_FILE}")

    return result


def load_and_clean_qs():
    """Load and clean QS rankings."""
    print("Loading QS rankings...")
    log_message("Starting QS data loading")
    df = pd.read_csv(QS_FILE)
    initial_count = len(df)
    log_message(f"QS CSV loaded: {initial_count} records")

    # Remove China-related institutions
    df = df[~df["Region"].apply(is_china_related)]
    china_filtered = initial_count - len(df)
    if china_filtered > 0:
        log_message(f"Filtered out {china_filtered} China-related QS records")

    # Normalize name and country
    df["name_normalized"] = df["Name"].apply(normalize_name)
    df["country"] = df["Region"].apply(normalize_country)
    df["status"] = df["Status"].apply(normalize_nature_of_running)

    df = df[
        [
            "Name",
            "country",
            "Rank",
            "status",
            "Latitude",
            "Longitude",
            "name_normalized",
        ]
    ]
    df.rename(columns={"Rank": "qs_rank"}, inplace=True)

    log_message(f"QS data cleaned: {len(df)} universities ready for merge", "INFO")
    print(f"  Loaded {len(df)} QS universities (after filtering China-related)")
    return df


def load_and_clean_the():
    """Load and clean THE rankings."""
    print("Loading THE rankings...")
    log_message("Starting THE data loading")
    df = pd.read_csv(THE_FILE)
    initial_count = len(df)
    log_message(f"THE CSV loaded: {initial_count} records")

    # Remove China-related institutions
    df = df[~df["Country"].apply(is_china_related)]
    china_filtered = initial_count - len(df)
    if china_filtered > 0:
        log_message(f"Filtered out {china_filtered} China-related THE records")

    # Normalize name and country
    df["name_normalized"] = df["Name"].apply(normalize_name)
    df["country"] = df["Country"].apply(normalize_country)
    df = df[["Name", "country", "Rank", "name_normalized"]]
    df.rename(columns={"Rank": "the_rank"}, inplace=True)

    log_message(f"THE data cleaned: {len(df)} universities ready for merge", "INFO")
    print(f"  Loaded {len(df)} THE universities (after filtering China-related)")
    return df


def load_and_clean_usnews():
    """Load and clean US News LAC rankings."""
    print("Loading US News LAC rankings...")
    log_message("Starting USNews LAC data loading")
    df = pd.read_csv(USNEWS_FILE)
    log_message(f"USNews LAC CSV loaded: {len(df)} records", "INFO")

    # Add country as United States for all LAC entries
    df["country"] = "United States of America"

    # Normalize name
    df["name_normalized"] = df["Name"].apply(normalize_name)
    df = df[["Name", "country", "Rank", "name_normalized"]]
    df.rename(columns={"Rank": "usnews_rank"}, inplace=True)

    log_message(f"USNews data cleaned: {len(df)} universities ready for merge", "INFO")
    print(f"  Loaded {len(df)} US News LAC universities")
    return df


def merge_datasets(qs_df, the_df, usnews_df):
    """
    Merge three datasets using exact name+country matching only.

    规则：
    1. 精确匹配: (country, normalized_name) 完全相同 → 合并数据
    2. 不存在精确匹配 → 作为新的学校添加（不进行模糊匹配）
    3. 避免假合并（如 Cornell University vs Cornell College）
    """
    print("\nMerging datasets...")
    log_message("Starting dataset merge process")

    # Start with QS as base
    print("  Starting with QS data...")
    log_message(f"Base dataset: QS with {len(qs_df)} universities")
    merged_list = qs_df.to_dict("records")

    # Create a mapping for faster lookups
    def create_lookup_map(records):
        """Create name+country lookup map."""
        lookup = {}
        for i, record in enumerate(records):
            key = (record["country"], record["name_normalized"])
            lookup[key] = i
        return lookup

    # Merge THE data
    print("  Merging THE data...")
    log_message(f"Merging THE dataset ({len(the_df)} universities)")
    qs_lookup = create_lookup_map(merged_list)

    the_matched = 0
    the_new = 0
    for _, the_row in the_df.iterrows():
        key = (the_row["country"], the_row["name_normalized"])

        # Only exact match, no fuzzy fallback
        if key in qs_lookup:
            merged_list[qs_lookup[key]]["the_rank"] = the_row["the_rank"]
            the_matched += 1
        else:
            # No match found, add as new university
            new_row = {
                "Name": the_row["Name"],
                "country": the_row["country"],
                "name_normalized": the_row["name_normalized"],
                "qs_rank": None,
                "the_rank": the_row["the_rank"],
                "usnews_rank": None,
                "status": None,
                "Latitude": None,
                "Longitude": None,
            }
            merged_list.append(new_row)
            the_new += 1

    log_message(
        f"THE merge complete: {the_matched} matched, {the_new} new universities added",
        "MERGE",
    )

    # Merge USNews data
    print("  Merging US News LAC data...")
    log_message(f"Merging USNews dataset ({len(usnews_df)} universities)")
    merged_lookup = create_lookup_map(merged_list)

    usnews_matched = 0
    usnews_new = 0
    for _, usnews_row in usnews_df.iterrows():
        key = (usnews_row["country"], usnews_row["name_normalized"])

        # Only exact match, no fuzzy fallback
        if key in merged_lookup:
            merged_list[merged_lookup[key]]["usnews_rank"] = usnews_row["usnews_rank"]
            usnews_matched += 1
        else:
            # No match found, add as new university
            new_row = {
                "Name": usnews_row["Name"],
                "country": usnews_row["country"],
                "name_normalized": usnews_row["name_normalized"],
                "qs_rank": None,
                "the_rank": None,
                "usnews_rank": usnews_row["usnews_rank"],
                "status": None,
                "Latitude": None,
                "Longitude": None,
            }
            merged_list.append(new_row)
            usnews_new += 1

    log_message(
        f"USNews merge complete: {usnews_matched} matched, {usnews_new} new universities added",
        "MERGE",
    )

    result_df = pd.DataFrame(merged_list)
    print(f"  Total merged records: {len(result_df)}")
    log_message(f"Dataset merge complete: {len(result_df)} total records")

    return result_df


def finalize_output(df):
    """
    Finalize and format output dataframe.
    """
    print("\nFinalizing output...")

    # Select and reorder columns
    output_columns = [
        "Name",
        "country",
        "qs_rank",
        "the_rank",
        "usnews_rank",
        "status",
        "Latitude",
        "Longitude",
    ]

    df = df[output_columns].copy()

    # Rename columns (status -> natureOfRunning)
    df.columns = [
        "Name",
        "Country",
        "QS_Rank",
        "THE_Rank",
        "USNews_Rank",
        "natureOfRunning",
        "Latitude",
        "Longitude",
    ]

    # Sort by QS rank (prefer QS as primary ranking)
    df = df.sort_values(by="QS_Rank", na_position="last")
    df = df.reset_index(drop=True)

    return df


def verify_ranking_data(original_qs, original_the, original_usnews, final_df):
    """
    Verify that all ranking data from original sources are preserved in the merged output.
    """
    print("\nVerifying ranking data integrity...")

    # Count rankings in original data
    qs_count = len(original_qs)
    the_count = len(original_the)
    usnews_count = len(original_usnews)

    # Count rankings in final output
    final_qs_count = final_df["QS_Rank"].notna().sum()
    final_the_count = final_df["THE_Rank"].notna().sum()
    final_usnews_count = final_df["USNews_Rank"].notna().sum()

    # Verify counts
    print(f"\n  QS Rankings:")
    print(f"    Original: {qs_count}, Final: {final_qs_count}", end="")
    if final_qs_count >= qs_count * 0.95:  # Allow 5% tolerance for filtering
        print(" [OK]")
    else:
        print(f" [FAIL] (Missing {qs_count - final_qs_count} rankings)")

    print(f"  THE Rankings:")
    print(f"    Original: {the_count}, Final: {final_the_count}", end="")
    if final_the_count >= the_count * 0.95:
        print(" [OK]")
    else:
        print(f" [FAIL] (Missing {the_count - final_the_count} rankings)")

    print(f"  USNews Rankings:")
    print(f"    Original: {usnews_count}, Final: {final_usnews_count}", end="")
    if final_usnews_count >= usnews_count * 0.95:
        print(" [OK]")
    else:
        print(f" [FAIL] (Missing {usnews_count - final_usnews_count} rankings)")

    print("\n  Overall integrity check:")
    total_original = qs_count + the_count + usnews_count
    total_final = final_qs_count + final_the_count + final_usnews_count
    preservation_rate = (total_final / total_original) * 100
    print(f"    Total original rankings: {total_original}")
    print(f"    Total preserved rankings: {total_final}")
    print(f"    Preservation rate: {preservation_rate:.1f}%")


def main():
    """Main function."""
    print("=" * 80)
    print("University Rankings Merger")
    print("=" * 80)

    try:
        # Load and clean data
        qs_df = load_and_clean_qs()
        the_df = load_and_clean_the()
        usnews_df = load_and_clean_usnews()

        # Store original counts for verification
        original_qs_count = len(qs_df)
        original_the_count = len(the_df)
        original_usnews_count = len(usnews_df)

        # Merge datasets
        merged_df = merge_datasets(qs_df, the_df, usnews_df)
        print(f"Total records before deduplication: {len(merged_df)}")

        # Finalize first to get proper column names
        output_df = finalize_output(merged_df)

        # Apply fuzzy deduplication
        output_df = deduplicate_fuzzy(output_df)

        # Save to CSV
        output_df.to_csv(MERGED_FILE, index=False)
        print(f"\n[OK] Merged data saved to: {MERGED_FILE}")

        # Verify ranking data integrity
        verify_ranking_data(qs_df, the_df, usnews_df, output_df)

        # Statistics
        print("\n" + "=" * 80)
        print("Statistics:")
        print("=" * 80)
        print(f"Total universities: {len(output_df)}")
        print(f"  - With QS rank: {output_df['QS_Rank'].notna().sum()}")
        print(f"  - With THE rank: {output_df['THE_Rank'].notna().sum()}")
        print(f"  - With USNews rank: {output_df['USNews_Rank'].notna().sum()}")
        print(f"  - With coordinates: {output_df['Latitude'].notna().sum()}")
        print(f"  - With nature info: {output_df['natureOfRunning'].notna().sum()}")

        # Nature breakdown
        nature_counts = output_df["natureOfRunning"].value_counts()
        print(f"\nInstitution type breakdown:")
        for nature, count in nature_counts.items():
            pct = count / len(output_df) * 100
            print(f"  - {nature}: {count} ({pct:.1f}%)")

        # Top countries
        print(f"\nTop 10 countries by university count:")
        top_countries = output_df["Country"].value_counts().head(10)
        for country, count in top_countries.items():
            print(f"  {country}: {count}")

        print("\n" + "=" * 80)
        print("[OK] Merge complete!")
        print("=" * 80)

        # Log completion
        log_message("Merge process completed successfully", "INFO")

        return 0

    except Exception as e:
        error_msg = f"Error: {e}"
        log_message(error_msg, "ERROR")
        print(f"\n✗ {error_msg}", file=__import__("sys").stderr)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
