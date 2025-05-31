import pandas as pd
import sqlite3
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm

# 맥에서 한글 폰트 설정
import platform

# 시스템 확인 및 폰트 설정
system = platform.system()
print(f"현재 시스템: {system}")

# 사용 가능한 폰트 출력
available_fonts = sorted([f.name for f in fm.fontManager.ttflist])
print("사용 가능한 폰트 중 한글 관련 폰트:")
korean_fonts = [font for font in available_fonts if any(keyword in font.lower() for keyword in ['nanum', 'malgun', 'apple', 'gothic', 'gulim', 'dotum', 'batang'])]
for font in korean_fonts[:10]:  # 상위 10개만 출력
    print(f"  - {font}")

# 맥용 한글 폰트 설정
if system == 'Darwin':  # macOS
    font_candidates = [
        'AppleSDGothicNeo-Regular',
        'AppleGothic', 
        'Apple SD Gothic Neo',
        'Helvetica',
        'Arial Unicode MS',
        'NanumGothic',
        'Nanum Gothic'
    ]
    
    font_set = False
    for font in font_candidates:
        if font in available_fonts:
            plt.rcParams['font.family'] = font
            print(f"✅ 한글 폰트 설정 완료: {font}")
            font_set = True
            break
    
    if not font_set:
        print("⚠️ 적절한 한글 폰트를 찾지 못했습니다.")
        # 대안: 시스템 기본 폰트 사용
        plt.rcParams['font.family'] = ['AppleGothic', 'DejaVu Sans']

else:
    # Windows/Linux용 설정
    font_candidates = ['Malgun Gothic', 'NanumGothic', 'DejaVu Sans']
    for font in font_candidates:
        if font in available_fonts:
            plt.rcParams['font.family'] = font
            break

# 마이너스 폰트 깨짐 방지
plt.rcParams['axes.unicode_minus'] = False

# 폰트 캐시 새로고침
fm.fontManager.addfont('/System/Library/Fonts/AppleSDGothicNeo.ttc') if system == 'Darwin' else None

# 파일 경로
db_path = "job_dev_rallit_1.db"
csv_path = "remember_sw.csv"

# SQLite에서 jobs 테이블 불러오기
conn = sqlite3.connect(db_path)
job_df = pd.read_sql("SELECT title, jobSkillKeywords FROM jobs", conn)
conn.close()

# CSV 불러오기 (CP949 인코딩)
csv_df = pd.read_csv(csv_path, encoding='cp949')
csv_df = csv_df[['title', 'job_description', 'job_rank_category', 'job_role']]
csv_df = csv_df.rename(columns={
    "job_description": "description",
    "job_rank_category": "rank",
    "job_role": "role"
})
job_df = job_df.rename(columns={"jobSkillKeywords": "job_keywords"})

# 병합 및 필터링 (소프트웨어 관련 공고 추출)
combined_df = pd.merge(csv_df, job_df, on="title", how="outer")
sw_df = combined_df[combined_df['title'].str.contains("SW|소프트웨어|소프트|백엔드|프론트엔드|Frontend|Backend|개발", case=False, na=False)]

# 결측치 처리
sw_df['job_keywords'] = sw_df['job_keywords'].fillna('')
sw_df['rank'] = sw_df['rank'].fillna('미지정')
sw_df['role'] = sw_df['role'].fillna('미지정')

# 1. 기술 키워드 분석
keywords_series = sw_df['job_keywords'].str.split(r"[,/|;·\n\s]+").dropna()
flat_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip()]
keyword_counts = Counter(flat_keywords)
top_keywords = dict(keyword_counts.most_common(15))

plt.figure(figsize=(12, 8))
plt.barh(list(top_keywords.keys()), list(top_keywords.values()))
plt.xlabel('빈도수', fontsize=12)
plt.title('주요 기술 키워드 상위 15개', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("기술키워드분석.png", dpi=300, bbox_inches='tight')
plt.close()

# 2. 직급(rank) 분포
rank_counts = sw_df['rank'].value_counts().head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x=rank_counts.values, y=rank_counts.index)
plt.xlabel("공고 수", fontsize=12)
plt.ylabel("직급", fontsize=12)
plt.title("직급별 공고 수 (상위 10)", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig("직급분석.png", dpi=300, bbox_inches='tight')
plt.close()

# 3. 역할(role) 분포
role_counts = sw_df['role'].value_counts().head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x=role_counts.values, y=role_counts.index)
plt.xlabel("공고 수", fontsize=12)
plt.ylabel("역할", fontsize=12)
plt.title("직무 역할별 공고 수 (상위 10)", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig("역할분석.png", dpi=300, bbox_inches='tight')
plt.close()

print("✅ 분석 완료! 그래프 3개가 PNG로 저장되었습니다.")
print(f"📊 총 SW 관련 공고 수: {len(sw_df)}개")
print(f"🔧 상위 5개 기술 키워드: {list(top_keywords.keys())[:5]}")
print(f"👔 상위 5개 직급: {rank_counts.head().index.tolist()}")
print(f"🎯 상위 5개 역할: {role_counts.head().index.tolist()}")