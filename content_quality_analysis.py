import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.font_manager as fm
import matplotlib
import platform

# 맥에서 한글 폰트 설정
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
if system == 'Darwin':
    try:
        fm.fontManager.addfont('/System/Library/Fonts/AppleSDGothicNeo.ttc')
    except:
        pass

# 파일 경로
db_path = "job_dev_rallit_1.db"
csv_path = "remember_sw.csv"

# 데이터 불러오기
conn = sqlite3.connect(db_path)
job_df = pd.read_sql("SELECT title, jobSkillKeywords, companyRepresentativeImage, isBookmarked FROM jobs", conn)
conn.close()

csv_df = pd.read_csv(csv_path, encoding='cp949')
csv_df = csv_df[['title', 'job_description', 'thumbnail_url']]
csv_df = csv_df.rename(columns={"job_description": "description"})

# 병합
df = pd.merge(csv_df, job_df, on="title", how="outer")

# 결측치 처리
df['description'] = df['description'].fillna('')
df['jobSkillKeywords'] = df['jobSkillKeywords'].fillna('')
df['thumbnail_url'] = df['thumbnail_url'].fillna('')
df['companyRepresentativeImage'] = df['companyRepresentativeImage'].fillna('')
df['isBookmarked'] = df['isBookmarked'].fillna(0).astype(int)

# ✅ 1. 공고 내용 길이 분석
df['desc_length'] = df['description'].str.len()

plt.figure(figsize=(10, 6))
sns.histplot(df['desc_length'], bins=30, kde=True)
plt.title("공고 내용 길이 분포", fontsize=14, fontweight='bold')
plt.xlabel("공고 설명 길이 (문자 수)", fontsize=12)
plt.ylabel("공고 수", fontsize=12)
plt.tight_layout()
plt.savefig("공고내용길이_분포.png", dpi=300, bbox_inches='tight')
plt.close()

# ✅ 2. 이미지 포함 여부에 따른 북마크 비율 분석
df['has_image'] = (df['companyRepresentativeImage'] != '') | (df['thumbnail_url'] != '')
image_group = df.groupby('has_image')['isBookmarked'].mean().reset_index()
image_group['has_image'] = image_group['has_image'].map({True: '이미지 있음', False: '이미지 없음'})

plt.figure(figsize=(8, 5))
sns.barplot(data=image_group, x='has_image', y='isBookmarked')
plt.title("이미지 포함 여부에 따른 북마크 비율", fontsize=14, fontweight='bold')
plt.ylabel("평균 북마크 비율", fontsize=12)
plt.xlabel("이미지 포함 여부", fontsize=12)
plt.tight_layout()
plt.savefig("이미지_북마크_비교.png", dpi=300, bbox_inches='tight')
plt.close()

# ✅ 3. 유사 공고 클러스터링 (title + description 유사도)
df['combined_text'] = df['title'].fillna('') + " " + df['description']
vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = vectorizer.fit_transform(df['combined_text'])
similarity_matrix = cosine_similarity(tfidf_matrix)

# 유사도 상위 쌍 추출
similar_pairs = []
for i in range(len(df)):
    for j in range(i + 1, len(df)):
        if similarity_matrix[i, j] > 0.9:
            similar_pairs.append((df.iloc[i]['title'], df.iloc[j]['title'], similarity_matrix[i, j]))

similar_df = pd.DataFrame(similar_pairs, columns=["공고1", "공고2", "유사도"])
similar_df.to_csv("유사공고_쌍.csv", index=False, encoding='utf-8-sig')

print("✅ 분석 완료! 결과 그래프 2개와 유사 공고 CSV가 생성되었습니다.")
print(f"📊 총 공고 수: {len(df)}개")
print(f"📝 평균 공고 설명 길이: {df['desc_length'].mean():.0f}자")
print(f"🖼️ 이미지 포함 공고 비율: {df['has_image'].mean()*100:.1f}%")
print(f"⭐ 전체 북마크 비율: {df['isBookmarked'].mean()*100:.1f}%")
if len(similar_df) > 0:
    print(f"🔗 유사 공고 쌍 발견: {len(similar_df)}개")
else:
    print("🔗 유사도 90% 이상인 공고 쌍이 없습니다.")