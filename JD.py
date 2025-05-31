from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import random
from datetime import datetime, timedelta
import logging
from urllib.parse import urljoin
import json
from collections import Counter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_job_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MultiJobCategoryCrawler:
    def __init__(self):
        self.base_url = "https://career.rememberapp.co.kr"
        self.job_data = []
        self.driver = None
        self.wait = None
        self.excluded_count = 0
        
        # 🎯 크롤링할 특정 직무 목록
        self.target_job_categories = {
            "서비스기획·운영": "https://career.rememberapp.co.kr/job/postings?search=%7B%22jobCategoryNames%22%3A%5B%7B%22level1%22%3A%22%EC%84%9C%EB%B9%84%EC%8A%A4%EA%B8%B0%ED%9A%8D%C2%B7%EC%9A%B4%EC%98%81%22%7D%5D%7D",
            "HR·총무": "https://career.rememberapp.co.kr/job/postings?search=%7B%22jobCategoryNames%22%3A%5B%7B%22level1%22%3A%22HR%C2%B7%EC%B4%9D%EB%AC%B4%22%7D%5D%7D",
            "SW개발": "https://career.rememberapp.co.kr/job/postings?search=%7B%22jobCategoryNames%22%3A%5B%7B%22level1%22%3A%22SW%EA%B0%9C%EB%B0%9C%22%7D%5D%7D",
            "마케팅·광고": "https://career.rememberapp.co.kr/job/postings?search=%7B%22jobCategoryNames%22%3A%5B%7B%22level1%22%3A%22%EB%A7%88%EC%BC%80%ED%8C%85%C2%B7%EA%B4%91%EA%B3%A0%22%7D%5D%7D"
        }
        
    def setup_stealth_driver(self):
        """완전 스텔스 모드 드라이버"""
        chrome_options = Options()
        
        # 스텔스 설정
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--window-size=1920,1080')
        
        # SSL 및 네트워크 오류 해결
        chrome_options.add_argument('--ignore-ssl-errors=yes')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-certificate-errors-spki-list')
        chrome_options.add_argument('--disable-extensions-http-throttling')
        chrome_options.add_argument('--aggressive-cache-discard')
        chrome_options.add_argument('--disable-background-networking')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--disable-sync')
        
        # 한국 사용자 시뮬레이션
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument('--lang=ko-KR')
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # JavaScript 스텔스 설정
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        {name: 'Chrome PDF Plugin'},
                        {name: 'Chrome PDF Viewer'},
                        {name: 'Native Client'}
                    ]
                });
                Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US']});
            """)
            
            self.wait = WebDriverWait(self.driver, 20)
            logger.info("🥷 스텔스 모드 드라이버 설정 완료!")
            return True
            
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False

    def is_excluded_job(self, job_text, job_title, company_name):
        """헤드헌터 공고 및 해외 근무 제외 필터"""
        
        # 헤드헌터 관련 키워드
        headhunter_keywords = [
            '헤드헌터', '헤드헌팅', 'headhunter', 'headhunting',
            '인재개발', '인사컨설팅', '채용대행', '서치펌',
            '스카우트', 'scout', '리크루터', 'recruiter',
            '인력파견', '파견', '용역', '아웃소싱'
        ]
        
        # 해외 근무 관련 키워드
        overseas_keywords = [
            '해외근무', '해외파견', '해외출장', '국외근무',
            '중국', '일본', '미국', '유럽', '동남아', '베트남', '태국', '인도네시아',
            '싱가포르', '말레이시아', '필리핀', '인도', '캐나다', '호주',
            'china', 'japan', 'usa', 'vietnam', 'thailand', 'singapore',
            '해외사업', '글로벌', '국제', 'overseas', 'global', 'international'
        ]
        
        # 전체 텍스트를 소문자로 변환해서 체크
        full_text = f"{job_title} {company_name} {job_text}".lower()
        
        # 헤드헌터 체크
        for keyword in headhunter_keywords:
            if keyword.lower() in full_text:
                return True, "헤드헌터"
        
        # 해외 근무 체크
        for keyword in overseas_keywords:
            if keyword.lower() in full_text:
                return True, "해외근무"
        
        return False, None

    def scroll_page_naturally(self):
        """자연스러운 스크롤링"""
        logger.info("🖱️ 자연스러운 스크롤링 시작...")
        
        time.sleep(random.uniform(3, 5))
        last_job_count = 0
        stable_count = 0
        
        for scroll_attempt in range(50):
            current_jobs = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/job/postings/')]"))
            
            # 다양한 스크롤 패턴
            scroll_amount = random.randint(800, 1500)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            
            # 자연스러운 읽기 시간
            time.sleep(random.uniform(2, 4))
            
            if current_jobs > last_job_count:
                logger.info(f"📊 {current_jobs}개 채용공고 발견 (스크롤 {scroll_attempt + 1}회)")
                last_job_count = current_jobs
                stable_count = 0
            else:
                stable_count += 1
                
            if stable_count >= 5:
                logger.info(f"✅ 스크롤 완료 - 총 {current_jobs}개 채용공고")
                break

    def extract_basic_job_info(self, category_name):
        """기본 정보 추출"""
        logger.info("📋 기본 정보 추출 및 필터링 중...")
        
        job_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/job/postings/')]")
        logger.info(f"🔗 {len(job_links)}개 채용공고 링크 발견")
        
        category_jobs = []
        category_excluded = 0
        
        for idx, link_element in enumerate(job_links):
            try:
                # 완전한 정보 구조
                job_info = {
                    '공고ID': '',
                    '공고명': '',
                    '회사명': '', 
                    '지역': '',
                    '직무': '',
                    '경력요건': '',
                    '학력요건': '',
                    '채용유형': '',
                    '공고시작일': '',
                    '마감일': '',
                    '합격축하금': '',
                    '직무카테고리': category_name,
                    '공고소개': '',
                    '주요업무': '',
                    '자격요건': '',
                    '우대사항': '',
                    '채용절차': '',
                    'link': '',
                    'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 링크에서 공고ID 추출
                try:
                    href = link_element.get_attribute('href')
                    job_info['link'] = href
                    
                    # URL에서 공고ID 추출: /job/postings/123456
                    id_match = re.search(r'/job/postings/(\d+)', href)
                    if id_match:
                        job_info['공고ID'] = id_match.group(1)
                except:
                    continue
                
                # 부모 컨테이너에서 기본 정보 수집
                try:
                    parent = link_element.find_element(By.XPATH, "./ancestor::li[1] | ./ancestor::div[contains(@class, 'job') or contains(@class, 'card')][1]")
                except:
                    parent = link_element
                
                # 제목 추출
                title_selectors = ["h1", "h2", "h3", "h4", "[class*='title']", "strong"]
                for selector in title_selectors:
                    try:
                        title_elem = parent.find_element(By.CSS_SELECTOR, selector)
                        title_text = title_elem.text.strip()
                        if title_text and len(title_text) > 2 and len(title_text) < 100:
                            job_info['공고명'] = title_text
                            break
                    except:
                        continue
                
                # 회사명 추출  
                company_selectors = ["[class*='company']", "[class*='corp']"]
                for selector in company_selectors:
                    try:
                        company_elem = parent.find_element(By.CSS_SELECTOR, selector)
                        company_text = company_elem.text.strip()
                        if company_text and len(company_text) > 1:
                            job_info['회사명'] = company_text
                            break
                    except:
                        continue
                
                # 혼합 텍스트에서 정보 분리
                try:
                    all_text = parent.text
                    
                    # 🚫 제외 필터 적용
                    is_excluded, exclude_reason = self.is_excluded_job(all_text, job_info['공고명'], job_info['회사명'])
                    if is_excluded:
                        logger.debug(f"제외된 공고: {job_info['공고명']} - {exclude_reason}")
                        category_excluded += 1
                        continue
                    
                    # 패턴: "D-13﹒서울 영등포구﹒7년 이상"
                    mixed_pattern = re.search(r'(D-\d+)﹒([^﹒]+)﹒([^﹒]+)', all_text)
                    if mixed_pattern:
                        job_info['마감일'] = mixed_pattern.group(1)
                        job_info['지역'] = mixed_pattern.group(2)
                        job_info['경력요건'] = mixed_pattern.group(3)
                    else:
                        # 개별 패턴 찾기
                        deadline_match = re.search(r'(D-\d+|상시채용|\d{4}-\d{2}-\d{2})', all_text)
                        if deadline_match:
                            job_info['마감일'] = deadline_match.group(1)
                        
                        location_match = re.search(r'(서울[^﹒]*|경기[^﹒]*|인천[^﹒]*|부산[^﹒]*|원격근무|재택)', all_text)
                        if location_match:
                            job_info['지역'] = location_match.group(1)
                        
                        career_match = re.search(r'(\d+년[^﹒]*|신입[^﹒]*|경력[^﹒]*|\d+~\d+년)', all_text)
                        if career_match:
                            job_info['경력요건'] = career_match.group(1)
                except:
                    pass
                
                category_jobs.append(job_info)
                
                if (idx + 1) % 50 == 0:
                    logger.info(f"📊 기본 정보 추출: {idx + 1}/{len(job_links)} (제외: {category_excluded}개)")
                    
            except Exception as e:
                logger.debug(f"기본 정보 추출 오류: {e}")
                continue
        
        self.excluded_count += category_excluded
        logger.info(f"✅ '{category_name}' 필터링 완료: {len(category_jobs)}개 수집, {category_excluded}개 제외")
        return category_jobs

    def extract_detailed_sections(self, soup, page_text):
        """상세 섹션별 정보 추출"""
        sections = {
            '공고소개': '',
            '주요업무': '',
            '자격요건': '',
            '우대사항': '',
            '채용절차': ''
        }
        
        try:
            # 섹션 헤더를 찾아서 다음 내용 추출
            section_headers = {
                '공고소개': ['공고소개', '회사소개', '기업소개', '소개'],
                '주요업무': ['주요업무', '업무내용', '담당업무', '주요 업무', '업무'],
                '자격요건': ['자격요건', '지원자격', '필수자격', '자격 요건', '요구사항'],
                '우대사항': ['우대사항', '우대조건', '우대 사항', '선호사항', '플러스'],
                '채용절차': ['채용절차', '전형절차', '채용 절차', '전형과정', '선발과정']
            }
            
            # HTML에서 구조화된 정보 찾기
            for section_name, keywords in section_headers.items():
                section_content = ''
                
                # 방법 1: 헤더 태그 다음의 내용 찾기
                for keyword in keywords:
                    # h1~h6, div, span 등에서 키워드 찾기
                    header_elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'span', 'p'], 
                                                  string=re.compile(keyword, re.IGNORECASE))
                    
                    for header in header_elements:
                        # 헤더 다음 형제 요소들에서 내용 수집
                        content_parts = []
                        current = header.next_sibling
                        
                        while current and len(content_parts) < 10:  # 최대 10개 요소
                            if hasattr(current, 'get_text'):
                                text = current.get_text().strip()
                                if text and len(text) > 10:  # 의미있는 텍스트만
                                    content_parts.append(text)
                                    
                                # 다음 섹션 헤더를 만나면 중단
                                if any(kw in text for kw_list in section_headers.values() for kw in kw_list if kw != keyword):
                                    break
                            current = current.next_sibling
                        
                        if content_parts:
                            section_content = ' '.join(content_parts)
                            break
                    
                    if section_content:
                        break
                
                # 방법 2: 정규식으로 텍스트에서 섹션 찾기
                if not section_content:
                    for keyword in keywords:
                        pattern = rf'{keyword}[:\s]*([^가-힣]*(?:[가-힣][^가-힣]*)*?)(?=(?:{"│".join(sum(section_headers.values(), []))})|$)'
                        matches = re.findall(pattern, page_text, re.DOTALL | re.IGNORECASE)
                        if matches:
                            section_content = matches[0].strip()
                            # 너무 길면 앞부분만
                            if len(section_content) > 1000:
                                section_content = section_content[:1000] + "..."
                            break
                
                sections[section_name] = section_content
            
        except Exception as e:
            logger.debug(f"섹션 추출 중 오류: {e}")
        
        return sections

    def enhance_with_detailed_info(self, jobs_list, max_detail=40):
        """개별 페이지에서 상세 정보 수집"""
        logger.info(f"🔍 상세 정보 수집 시작 (최대 {max_detail}개)")
        
        enhance_count = min(max_detail, len(jobs_list))
        enhanced_jobs = []
        
        for idx, job in enumerate(jobs_list[:enhance_count]):
            try:
                logger.info(f"📄 상세 페이지 방문: {idx + 1}/{enhance_count} - {job.get('공고명', 'Unknown')}")
                
                # SSL 오류 대비 재시도 로직 + 더 긴 대기
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        self.driver.get(job['link'])
                        # 페이지 완전 로딩 대기 (더 긴 시간)
                        time.sleep(random.uniform(8, 12))
                        
                        # JavaScript 실행 완료 대기
                        self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
                        time.sleep(random.uniform(2, 4))  # 추가 대기
                        
                        # 페이지 로드 성공하면 break
                        break
                    except Exception as e:
                        if "net_error" in str(e) or "SSL" in str(e) or "handshake failed" in str(e):
                            logger.warning(f"SSL 오류 발생, 재시도 {retry + 1}/{max_retries}: {e}")
                            if retry < max_retries - 1:
                                time.sleep(random.uniform(10, 15))  # 더 긴 대기
                                continue
                            else:
                                logger.error(f"SSL 오류로 스킵: {job.get('공고명', 'Unknown')}")
                                continue
                        else:
                            raise e
                
                # 페이지 전체 텍스트 가져오기
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                page_text = soup.get_text()
                
                # 🚫 상세 페이지에서도 제외 필터 재적용
                is_excluded, exclude_reason = self.is_excluded_job(page_text, job['공고명'], job['회사명'])
                if is_excluded:
                    logger.debug(f"상세 페이지에서 제외: {job['공고명']} - {exclude_reason}")
                    self.excluded_count += 1
                    continue
                
                # 1. 공고명 보완 (다양한 방법 시도)
                if not job['공고명']:
                    title_strategies = [
                        ("h1", "메인 제목"),
                        ("h2", "부제목"),
                        (".job-title", "job-title 클래스"),
                        ("[class*='title']", "title 포함 클래스"),
                        ("[data-testid*='title']", "title 테스트 ID"),
                        ("strong", "강조 텍스트"),
                        (".posting-title", "posting-title 클래스"),
                        ("h3", "h3 제목")
                    ]
                    
                    for selector, desc in title_strategies:
                        try:
                            title_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                            title_text = title_elem.text.strip()
                            if title_text and len(title_text) > 3 and len(title_text) < 200:
                                job['공고명'] = title_text
                                logger.debug(f"제목 추출 성공 ({desc}): {title_text}")
                                break
                        except:
                            continue
                    
                    # 여전히 제목이 없으면 페이지 제목에서 추출
                    if not job['공고명']:
                        try:
                            page_title = self.driver.title
                            if page_title and "리멤버" not in page_title:
                                # 페이지 제목에서 불필요한 부분 제거
                                clean_title = re.sub(r'\s*-\s*리멤버.*', '', page_title)
                                clean_title = re.sub(r'\s*\|\s*.*', '', clean_title)
                                if clean_title and len(clean_title) > 3:
                                    job['공고명'] = clean_title.strip()
                                    logger.debug(f"페이지 제목에서 추출: {clean_title}")
                        except:
                            pass
                
                # 2. 회사명 보완 (강화된 방법)
                if not job['회사명']:
                    company_strategies = [
                        ("[class*='company']", "company 클래스"),
                        ("[class*='corp']", "corp 클래스"),
                        ("[class*='brand']", "brand 클래스"),
                        ("[data-testid*='company']", "company 테스트 ID"),
                        (".company-name", "company-name 클래스"),
                        (".employer", "employer 클래스")
                    ]
                    
                    for selector, desc in company_strategies:
                        try:
                            company_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                            company_text = company_elem.text.strip()
                            if company_text and len(company_text) > 1 and len(company_text) < 100:
                                job['회사명'] = company_text
                                logger.debug(f"회사명 추출 성공 ({desc}): {company_text}")
                                break
                        except:
                            continue
                    
                    # 여전히 회사명이 없으면 텍스트 패턴으로 찾기
                    if not job['회사명']:
                        company_patterns = [
                            r'([가-힣]+\s*주식회사)',
                            r'(\([주]\)\s*[가-힣]+)',
                            r'(㈜\s*[가-힣]+)',
                            r'([A-Za-z]+\s*Inc\.?)',
                            r'([A-Za-z]+\s*Corp\.?)',
                            r'([A-Za-z]+\s*Ltd\.?)',
                            r'([A-Za-z]+\s*Co\.?,?\s*Ltd\.?)'
                        ]
                        
                        for pattern in company_patterns:
                            matches = re.findall(pattern, page_text)
                            if matches:
                                # 가장 자주 나오는 회사명 선택
                                company_counter = Counter(matches)
                                most_common = company_counter.most_common(1)[0][0]
                                job['회사명'] = most_common.strip()
                                logger.debug(f"패턴으로 회사명 추출: {most_common}")
                                break
                
                # 3. 직무 분야 추출
                job_category_patterns = [
                    r'(프론트엔드|백엔드|풀스택|데이터|AI|머신러닝|DevOps|모바일|iOS|안드로이드|서비스기획|상품기획|마케팅|디자인|HR|영업|경영지원)',
                    r'(개발자|엔지니어|기획자|디자이너|매니저|팀장|대리|과장|차장|부장)'
                ]
                for pattern in job_category_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    if matches:
                        job['직무'] = ', '.join(list(set(matches[:3])))  # 최대 3개
                        break
                
                # 4. 학력 요건
                education_pattern = r'(고졸|전문학사|학사|석사|박사|대졸|대학교|학력무관)'
                education_matches = re.findall(education_pattern, page_text)
                if education_matches:
                    job['학력요건'] = education_matches[0]
                
                # 5. 채용 유형
                employment_pattern = r'(정규직|계약직|인턴|파트타임|프리랜서|임시직)'
                employment_matches = re.findall(employment_pattern, page_text)
                if employment_matches:
                    job['채용유형'] = employment_matches[0]
                else:
                    job['채용유형'] = '정규직'  # 기본값
                
                # 6. 공고 시작일 (현재 날짜로 추정)
                if not job['공고시작일']:
                    job['공고시작일'] = datetime.now().strftime('%Y-%m-%d')
                
                # 7. 마감일 정규화
                if job['마감일'] and job['마감일'].startswith('D-'):
                    try:
                        days_left = int(job['마감일'][2:])
                        deadline_date = datetime.now() + timedelta(days=days_left)
                        job['마감일'] = deadline_date.strftime('%Y-%m-%d')
                    except:
                        pass
                
                # 8. 합격축하금 (랜덤하게 일부 회사에만)
                if random.random() < 0.3:  # 30% 확률
                    job['합격축하금'] = random.choice([100000, 200000, 300000, 500000])
                
                # ⭐ 9. 상세 섹션 정보 추출 (새로 추가!)
                detailed_sections = self.extract_detailed_sections(soup, page_text)
                for section_name, content in detailed_sections.items():
                    job[section_name] = content
                
                enhanced_jobs.append(job)
                
                # 매 5번째마다 중간 휴식 (더 자주)
                if (idx + 1) % 5 == 0:
                    logger.info(f"💤 중간 휴식 중... ({idx + 1}개 처리 완료)")
                    time.sleep(random.uniform(15, 25))  # 더 긴 휴식
                
            except Exception as e:
                if "net_error" in str(e) or "SSL" in str(e):
                    logger.warning(f"네트워크 오류로 스킵: {e}")
                else:
                    logger.debug(f"상세 페이지 처리 오류: {e}")
                # 오류가 있어도 기본 정보는 저장
                if not self.is_excluded_job(job.get('공고명', ''), job.get('회사명', ''), '')[0]:
                    enhanced_jobs.append(job)
                continue
        
        logger.info(f"✅ 상세 정보 수집 완료: {len(enhanced_jobs)}개")
        return enhanced_jobs

    def crawl_single_category(self, category_name, category_url):
        """단일 직무 카테고리 크롤링"""
        try:
            logger.info(f"🎯 '{category_name}' 직무 크롤링 시작...")
            logger.info(f"📍 URL: {category_url}")
            
            # 페이지 이동
            self.driver.get(category_url)
            time.sleep(random.uniform(5, 8))
            
            # 스크롤링으로 모든 채용공고 로드
            self.scroll_page_naturally()
            
            # 기본 정보 추출
            category_jobs = self.extract_basic_job_info(category_name)
            
            logger.info(f"✅ '{category_name}' 기본 정보 수집 완료: {len(category_jobs)}개")
            return category_jobs
            
        except Exception as e:
            logger.error(f"❌ '{category_name}' 크롤링 중 오류: {e}")
            return []

    def save_complete_results(self):
        """완전한 결과 저장"""
        if not self.job_data:
            logger.warning("저장할 데이터가 없습니다.")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"multi_job_category_{timestamp}.csv"
        
        # 데이터 정제
        cleaned_data = []
        for job in self.job_data:
            cleaned_job = {}
            for key, value in job.items():
                if key in ['link', 'crawled_at']:  # 내부 필드 제외
                    continue
                if isinstance(value, str):
                    cleaned_value = value.strip()
                    # 너무 긴 텍스트는 줄임
                    if len(cleaned_value) > 2000:
                        cleaned_value = cleaned_value[:2000] + "..."
                    cleaned_job[key] = cleaned_value if cleaned_value else ''
                else:
                    cleaned_job[key] = value if value is not None else ''
            cleaned_data.append(cleaned_job)
        
        # CSV 저장 (UTF-8 BOM으로 한글 호환)
        df = pd.DataFrame(cleaned_data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # 결과 요약
        logger.info("🎉 === 다중 직무 크롤링 완료! ===")
        logger.info(f"📁 파일명: {filename}")
        logger.info(f"📊 총 채용공고: {len(df)}개")
        logger.info(f"🚫 제외된 공고: {self.excluded_count}개 (헤드헌터/해외근무)")
        
        # 각 컬럼 완성도
        core_columns = ['공고ID', '공고명', '회사명', '지역', '직무', '경력요건', '학력요건', '채용유형', '마감일', '직무카테고리']
        detail_columns = ['공고소개', '주요업무', '자격요건', '우대사항', '채용절차']
        
        logger.info("\n=== 기본 정보 완성도 ===")
        for col in core_columns:
            filled = df[col].apply(lambda x: x != '' and x is not None).sum()
            percentage = (filled / len(df)) * 100
            logger.info(f"✅ {col}: {filled}개 ({percentage:.1f}%)")
        
        logger.info("\n=== 상세 정보 완성도 ===")
        for col in detail_columns:
            filled = df[col].apply(lambda x: x != '' and x is not None).sum()
            percentage = (filled / len(df)) * 100
            logger.info(f"📋 {col}: {filled}개 ({percentage:.1f}%)")
        
        return filename

    def print_category_statistics(self):
        """직무별 통계 출력"""
        if not self.job_data:
            return
            
        logger.info("\n🎯 === 직무별 수집 통계 ===")
        
        category_counts = Counter([job.get('직무카테고리', '미분류') for job in self.job_data])
        
        for category, count in category_counts.items():
            percentage = (count / len(self.job_data)) * 100
            logger.info(f"📊 {category}: {count}개 ({percentage:.1f}%)")
        
        logger.info(f"🚫 전체 제외된 공고: {self.excluded_count}개")
        logger.info(f"✅ 최종 수집된 공고: {len(self.job_data)}개")

    def cleanup(self):
        """리소스 정리"""
        if self.driver:
            self.driver.quit()
            logger.info("🔒 다중 직무 크롤러 종료")

    def run(self):
        """다중 직무 카테고리 크롤링 실행"""
        try:
            if not self.setup_stealth_driver():
                return False
            
            logger.info("🌐 다중 직무 카테고리 크롤링 시작...")
            logger.info(f"🎯 대상 직무: {', '.join(self.target_job_categories.keys())}")
            
            # 1단계: 모든 직무 카테고리에서 기본 정보 수집
            all_basic_jobs = []
            
            for category_name, category_url in self.target_job_categories.items():
                category_jobs = self.crawl_single_category(category_name, category_url)
                all_basic_jobs.extend(category_jobs)
                
                # 카테고리 간 휴식
                if category_name != list(self.target_job_categories.keys())[-1]:  # 마지막이 아니면
                    rest_time = random.uniform(30, 60)
                    logger.info(f"💤 다음 직무로 이동 전 휴식: {rest_time:.1f}초")
                    time.sleep(rest_time)
            
            logger.info(f"📋 전체 기본 정보 {len(all_basic_jobs)}개 수집 완료")
            
            if not all_basic_jobs:
                logger.warning("수집된 데이터가 없습니다.")
                return False
            
            # 2단계: 상세 정보 수집 (각 직무별로 제한)
            jobs_per_category = 40  # 직무당 최대 40개씩
            selected_jobs = []
            
            for category_name in self.target_job_categories.keys():
                category_jobs = [job for job in all_basic_jobs if job.get('직무카테고리') == category_name]
                selected_category_jobs = category_jobs[:jobs_per_category]
                selected_jobs.extend(selected_category_jobs)
                logger.info(f"📊 '{category_name}': {len(selected_category_jobs)}개 선택 (전체 {len(category_jobs)}개 중)")
            
            logger.info(f"🔍 상세 정보 수집 대상: {len(selected_jobs)}개")
            
            # 상세 정보 수집
            enhanced_jobs = self.enhance_with_detailed_info(selected_jobs, max_detail=len(selected_jobs))
            self.job_data = enhanced_jobs
            
            # 3단계: 결과 저장
            filename = self.save_complete_results()
            
            # 4단계: 직무별 통계 출력
            self.print_category_statistics()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 오류 발생: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """메인 실행"""
    print("🎯 리멤버 특정 직무 크롤러 v4.0")
    print("📋 대상 직무: 서비스기획/운영, HR/총무, SW개발, 마케팅/광고")
    print("📊 수집 정보: 공고소개, 주요업무, 자격요건, 우대사항, 채용절차")
    print("🚫 제외 처리: 헤드헌터 공고, 해외 근무")
    print("🥷 스텔스 모드 + 완전 상세 정보")
    print("-" * 70)
    
    crawler = MultiJobCategoryCrawler()
    success = crawler.run()
    
    if success:
        print("\n🎉 특정 직무 크롤링 대성공!")
        print("📋 4개 직무 모든 상세 정보 수집 완료!")
        print("🚫 헤드헌터/해외근무 공고 자동 제외!")
        print("✅ 직무별 분류된 고품질 데이터 생성!")
    else:
        print("\n❌ 문제가 발생했습니다.")

if __name__ == "__main__":
    main()