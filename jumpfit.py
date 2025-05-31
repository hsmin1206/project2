import requests
import pandas as pd
import time
import random
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from pathlib import Path
import urllib.parse
from dataclasses import dataclass, asdict

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jumpit_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class JobPosting:
    """채용공고 데이터 클래스"""
    position_id: str = ""
    title: str = ""
    company_name: str = ""
    company_id: str = ""
    location: str = ""
    career_level: str = ""
    employment_type: str = ""
    salary: str = ""
    tech_stacks: str = ""
    benefits: str = ""
    job_category: str = ""
    description: str = ""
    requirements: str = ""
    preferred_qualifications: str = ""
    company_description: str = ""
    company_size: str = ""
    company_industry: str = ""
    posted_date: str = ""
    deadline: str = ""
    application_count: int = 0
    view_count: int = 0
    bookmark_count: int = 0
    response_rate: float = 0.0
    tags: str = ""
    work_location_type: str = ""
    experience_years: str = ""
    education_level: str = ""
    crawled_at: str = ""
    api_url: str = ""

class JumpitCrawler:
    def __init__(self, db_name: str = "jumpit_jobs.db"):
        self.base_api_url = "https://jumpit-api.saramin.co.kr/api/positions"
        self.session = requests.Session()
        self.db_name = db_name
        self.job_data: List[JobPosting] = []
        
        # 요청 헤더 설정 (한국 사용자 시뮬레이션)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.jumpit.co.kr/',
            'Origin': 'https://www.jumpit.co.kr',
            'DNT': '1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        }
        self.session.headers.update(self.headers)
        
        # 점핏 실제 직무 분류에 맞춘 검색 파라미터
        self.search_params = {
            # 🎯 핵심 개발 직무 (점핏 실제 카테고리명 사용)
            "서버/백엔드개발자": {"jobCategory": "서버/백엔드 개발자", "sort": "rsp_rate"},
            "프론트엔드개발자": {"jobCategory": "프론트엔드 개발자", "sort": "rsp_rate"},
            "웹풀스택개발자": {"jobCategory": "웹 풀스택 개발자", "sort": "rsp_rate"},
            "안드로이드개발자": {"jobCategory": "안드로이드 개발자", "sort": "rsp_rate"},
            "iOS개발자": {"jobCategory": "iOS 개발자", "sort": "rsp_rate"},
            
            # 🔧 시스템/인프라
            "크로스플랫폼앱개발자": {"jobCategory": "크로스플랫폼 앱개발자", "sort": "rsp_rate"},
            "게임클라이언트개발자": {"jobCategory": "게임 클라이언트 개발자", "sort": "rsp_rate"},
            "게임서버개발자": {"jobCategory": "게임 서버 개발자", "sort": "rsp_rate"},
            "DBA": {"jobCategory": "DBA", "sort": "rsp_rate"},
            "DevOps시스템엔지니어": {"jobCategory": "devops/시스템 엔지니어", "sort": "rsp_rate"},
            
            # 📊 데이터/AI
            "빅데이터엔지니어": {"jobCategory": "빅데이터 엔지니어", "sort": "rsp_rate"},
            "인공지능머신러닝": {"jobCategory": "인공지능/머신러닝", "sort": "rsp_rate"},
            
            # 🎨 기획/디자인/관리
            "정보보안담당자": {"jobCategory": "정보보안 담당자", "sort": "rsp_rate"},
            "QA엔지니어": {"jobCategory": "QA 엔지니어", "sort": "rsp_rate"},
            "개발PM": {"jobCategory": "개발 PM", "sort": "rsp_rate"},
            "HW임베디드": {"jobCategory": "HW/임베디드", "sort": "rsp_rate"},
            "SW솔루션": {"jobCategory": "SW/솔루션", "sort": "rsp_rate"},
            "웹퍼블리셔": {"jobCategory": "웹퍼블리셔", "sort": "rsp_rate"},
            "VR_AR_3D": {"jobCategory": "VR/AR/3D", "sort": "rsp_rate"},
            "블록체인": {"jobCategory": "블록체인", "sort": "rsp_rate"},
            "기술지원": {"jobCategory": "기술지원", "sort": "rsp_rate"},
            
            # 🔍 특별 검색 조건 (실제 점핏 파라미터)
            "재택근무가능": {"workFromHome": "true", "sort": "reg_dt"},
            "신입환영": {"newcomer": "true", "sort": "reg_dt"},
            "고연봉_5천이상": {"minSalary": "5000", "sort": "salary"},
            "고연봉_1억이상": {"minSalary": "10000", "sort": "salary"},
            "경력3년이상": {"minCareer": "3", "sort": "rsp_rate"},
            "경력5년이상": {"minCareer": "5", "sort": "rsp_rate"},
            "상시채용": {"alwaysOpen": "true", "sort": "reg_dt"},
            
            # 💻 주요 기술스택별 검색
            "Java스택": {"techStack": "Java", "sort": "rsp_rate"},
            "Python스택": {"techStack": "Python", "sort": "rsp_rate"},
            "JavaScript스택": {"techStack": "JavaScript", "sort": "rsp_rate"},
            "React스택": {"techStack": "React", "sort": "rsp_rate"},
            "Vue스택": {"techStack": "Vue.js", "sort": "rsp_rate"},
            "SpringBoot스택": {"techStack": "Spring Boot", "sort": "rsp_rate"},
            "NodeJS스택": {"techStack": "Node.js", "sort": "rsp_rate"},
            "Docker스택": {"techStack": "Docker", "sort": "rsp_rate"},
            "Kubernetes스택": {"techStack": "Kubernetes", "sort": "rsp_rate"},
            "AWS스택": {"techStack": "AWS", "sort": "rsp_rate"},
            "MongoDB스택": {"techStack": "MongoDB", "sort": "rsp_rate"},
            "PostgreSQL스택": {"techStack": "PostgreSQL", "sort": "rsp_rate"},
            
            # 🏢 지역별 검색
            "서울_강남": {"location": "서울 강남구", "sort": "reg_dt"},
            "서울_성수": {"location": "서울 성동구", "sort": "reg_dt"},
            "판교_분당": {"location": "경기 성남시", "sort": "reg_dt"},
            "부산지역": {"location": "부산", "sort": "reg_dt"}
        }
        
        # 데이터베이스 초기화
        self.init_database()
        
    def init_database(self):
        """SQLite 데이터베이스 초기화"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # 채용공고 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_postings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT UNIQUE,
                    title TEXT,
                    company_name TEXT,
                    company_id TEXT,
                    location TEXT,
                    career_level TEXT,
                    employment_type TEXT,
                    salary TEXT,
                    tech_stacks TEXT,
                    benefits TEXT,
                    job_category TEXT,
                    description TEXT,
                    requirements TEXT,
                    preferred_qualifications TEXT,
                    company_description TEXT,
                    company_size TEXT,
                    company_industry TEXT,
                    posted_date TEXT,
                    deadline TEXT,
                    application_count INTEGER,
                    view_count INTEGER,
                    bookmark_count INTEGER,
                    response_rate REAL,
                    tags TEXT,
                    work_location_type TEXT,
                    experience_years TEXT,
                    education_level TEXT,
                    crawled_at TEXT,
                    api_url TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 크롤링 로그 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crawling_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    search_type TEXT,
                    total_found INTEGER,
                    successfully_crawled INTEGER,
                    failed_requests INTEGER,
                    start_time TEXT,
                    end_time TEXT,
                    duration_seconds REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"✅ 데이터베이스 '{self.db_name}' 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
    
    def make_safe_request(self, url: str, params: Dict = None, max_retries: int = 3) -> Optional[Dict]:
        """안전한 API 요청 (재시도 로직 포함)"""
        for attempt in range(max_retries):
            try:
                # 요청 전 랜덤 대기 (서버 부하 방지)
                delay = random.uniform(2, 5) + (attempt * 2)  # 재시도 시 더 긴 대기
                time.sleep(delay)
                
                logger.info(f"🔄 API 요청 중... (시도 {attempt + 1}/{max_retries})")
                logger.debug(f"URL: {url}")
                logger.debug(f"Params: {params}")
                
                response = self.session.get(url, params=params, timeout=30)
                
                # 상태 코드 확인
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"✅ API 요청 성공 (응답 크기: {len(response.content)} bytes)")
                    return data
                elif response.status_code == 429:  # Too Many Requests
                    wait_time = random.uniform(30, 60)
                    logger.warning(f"⚠️ Rate limit 도달, {wait_time:.1f}초 대기...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"⚠️ HTTP {response.status_code}: {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⏰ 요청 타임아웃 (시도 {attempt + 1})")
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"🔌 연결 오류 (시도 {attempt + 1}): {e}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"📡 요청 오류 (시도 {attempt + 1}): {e}")
            except json.JSONDecodeError as e:
                logger.warning(f"📄 JSON 파싱 오류 (시도 {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                wait_time = random.uniform(10, 20) * (attempt + 1)
                logger.info(f"💤 {wait_time:.1f}초 후 재시도...")
                time.sleep(wait_time)
        
        logger.error("❌ 모든 재시도 실패")
        return None
    
    def parse_job_posting(self, job_data: Dict, search_type: str = "") -> JobPosting:
        """실제 점핏 API 응답 구조에 맞춘 JobPosting 객체 변환"""
        try:
            # 안전한 데이터 추출
            def safe_get(data, key, default=""):
                try:
                    result = data.get(key, default)
                    return str(result) if result is not None else default
                except (TypeError, AttributeError):
                    return default
            
            def safe_get_int(data, key, default=0):
                try:
                    result = data.get(key, default)
                    return int(result) if result is not None else default
                except (TypeError, ValueError, AttributeError):
                    return default
            
            def safe_get_float(data, key, default=0.0):
                try:
                    result = data.get(key, default)
                    return float(result) if result is not None else default
                except (TypeError, ValueError, AttributeError):
                    return default
            
            def safe_get_list(data, key, default=""):
                try:
                    result = data.get(key, [])
                    if isinstance(result, list):
                        return ", ".join([str(item) for item in result if item])
                    return str(result) if result is not None else default
                except (TypeError, AttributeError):
                    return default
            
            # 🎯 점핏 실제 API 응답 구조에 맞춘 파싱
            
            # 기본 정보
            position_id = safe_get(job_data, "id")
            title = safe_get(job_data, "title")
            company_name = safe_get(job_data, "companyName")
            job_category = safe_get(job_data, "jobCategory")
            
            # 위치 정보 (배열 형태)
            locations = job_data.get("locations", [])
            location = ", ".join(locations) if isinstance(locations, list) else str(locations)
            
            # 기술 스택 (배열 형태)
            tech_stacks = job_data.get("techStacks", [])
            tech_stacks_str = ", ".join(tech_stacks) if isinstance(tech_stacks, list) else str(tech_stacks)
            
            # 경력 정보
            min_career = safe_get_int(job_data, "minCareer")
            max_career = safe_get_int(job_data, "maxCareer")
            is_newcomer = job_data.get("newcomer", False)
            
            # 경력 요건 문자열 생성
            if is_newcomer:
                career_level = "신입 환영"
            elif min_career == 0 and max_career == 0:
                career_level = "경력무관"
            elif min_career == max_career:
                career_level = f"{min_career}년"
            else:
                career_level = f"{min_career}~{max_career}년"
            
            # 통계 정보
            view_count = safe_get_int(job_data, "viewCount")
            scrap_count = safe_get_int(job_data, "scrapCount")
            celebration = safe_get_int(job_data, "celebration")  # 합격축하금
            
            # 날짜 정보
            closed_at = safe_get(job_data, "closedAt")
            always_open = job_data.get("alwaysOpen", False)
            
            # 마감일 처리
            deadline = ""
            if always_open:
                deadline = "상시채용"
            elif closed_at:
                try:
                    # ISO 형식 날짜를 일반 형식으로 변환
                    from datetime import datetime
                    dt = datetime.fromisoformat(closed_at.replace('T', ' ').replace('Z', ''))
                    deadline = dt.strftime('%Y-%m-%d')
                except:
                    deadline = closed_at
            
            # 이미지 및 로고
            image_path = safe_get(job_data, "imagePath")
            logo_path = safe_get(job_data, "logo")
            
            # 숨겨진 공고 여부
            hidden_position = job_data.get("hiddenPosition", False)
            
            # 지원 여부
            applied = job_data.get("applied", False)
            scraped = job_data.get("scraped", False)
            
            posting = JobPosting(
                position_id=str(position_id),
                title=title,
                company_name=company_name,
                company_id=safe_get(job_data, "serialNumber"),
                location=location,
                career_level=career_level,
                employment_type="정규직",  # 기본값 (API에서 제공되지 않음)
                salary=f"{celebration}만원" if celebration > 0 else "",
                tech_stacks=tech_stacks_str,
                benefits="",  # API에서 제공되지 않음
                job_category=job_category,
                description="",  # 상세 페이지에서 가져와야 함
                requirements="",  # 상세 페이지에서 가져와야 함
                preferred_qualifications="",  # 상세 페이지에서 가져와야 함
                company_description="",  # 상세 페이지에서 가져와야 함
                company_size="",  # API에서 제공되지 않음
                company_industry="",  # API에서 제공되지 않음
                posted_date="",  # API에서 제공되지 않음
                deadline=deadline,
                application_count=0,  # API에서 제공되지 않음
                view_count=view_count,
                bookmark_count=scrap_count,
                response_rate=0.0,  # API에서 제공되지 않음
                tags="신입환영" if is_newcomer else "",
                work_location_type="",  # API에서 제공되지 않음
                experience_years=f"{min_career}-{max_career}년",
                education_level="",  # API에서 제공되지 않음
                crawled_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                api_url=search_type
            )
            
            return posting
            
        except Exception as e:
            logger.error(f"❌ 채용공고 파싱 오류: {e}")
            logger.debug(f"문제 데이터: {job_data}")
            return JobPosting()
    
    def crawl_search_type(self, search_name: str, params: Dict, max_pages: int = 10) -> List[JobPosting]:
        """특정 검색 조건으로 채용공고 크롤링"""
        logger.info(f"🎯 '{search_name}' 검색 시작...")
        logger.info(f"📋 검색 파라미터: {params}")
        
        search_jobs = []
        page = 1
        total_found = 0
        failed_requests = 0
        
        # 기본 파라미터 설정
        base_params = {
            "highlight": "false",
            "page": page,
            "limit": 20  # 한 번에 가져올 공고 수
        }
        base_params.update(params)
        
        while page <= max_pages:
            logger.info(f"📄 페이지 {page}/{max_pages} 처리 중...")
            
            base_params["page"] = page
            
            # API 요청
            response_data = self.make_safe_request(self.base_api_url, base_params)
            
            if not response_data:
                failed_requests += 1
                logger.warning(f"⚠️ 페이지 {page} 요청 실패")
                if failed_requests >= 3:  # 연속 3번 실패 시 중단
                    logger.error("❌ 연속 실패로 검색 중단")
                    break
                page += 1
                continue
            
            # 응답 데이터 구조 확인 - 점핏 실제 API 구조
            jobs_list = []
            total_count = 0
            current_page = page
            
            # 점핏 API 응답 구조: {result: {totalCount, page, positions: [...]}}
            if "result" in response_data:
                result_data = response_data["result"]
                
                # 총 개수 확인
                total_count = result_data.get("totalCount", 0)
                current_page = result_data.get("page", page)
                
                # positions 배열에서 채용공고 목록 가져오기
                if "positions" in result_data:
                    jobs_list = result_data["positions"]
                
                logger.info(f"📊 API 응답: 총 {total_count}개 중 페이지 {current_page}")
                
            # 백업: 다른 응답 구조도 처리
            elif "positions" in response_data:
                jobs_list = response_data["positions"]
                total_count = response_data.get("totalCount", len(jobs_list))
            elif "data" in response_data:
                if isinstance(response_data["data"], list):
                    jobs_list = response_data["data"]
                elif "positions" in response_data["data"]:
                    jobs_list = response_data["data"]["positions"]
            elif isinstance(response_data, list):
                jobs_list = response_data
            
            if not jobs_list:
                logger.info(f"📭 페이지 {page}에서 더 이상 공고를 찾을 수 없음")
                break
            
            logger.info(f"📊 페이지 {page}에서 {len(jobs_list)}개 공고 발견 (전체 {total_count}개)")
            total_found += len(jobs_list)
            
            # 각 채용공고 파싱
            page_jobs = []
            for job_data in jobs_list:
                try:
                    # 숨겨진 공고는 제외
                    if job_data.get("hiddenPosition", False):
                        continue
                        
                    job_posting = self.parse_job_posting(job_data, search_name)
                    if job_posting.position_id:  # 유효한 데이터인 경우
                        page_jobs.append(job_posting)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 공고 파싱 오류: {e}")
                    continue
            
            search_jobs.extend(page_jobs)
            logger.info(f"✅ 페이지 {page}: {len(page_jobs)}개 파싱 완료")
            
            # 페이지네이션 확인 - 점핏 스타일
            has_more = False
            
            # 1. totalCount 기반 판단
            if total_count > 0:
                items_per_page = base_params.get("limit", 20)
                max_possible_pages = (total_count + items_per_page - 1) // items_per_page
                has_more = page < max_possible_pages and page < max_pages
                
            # 2. 현재 페이지 결과 기반 판단
            elif len(jobs_list) == base_params.get("limit", 20):
                has_more = page < max_pages
                
            # 3. 빈 결과 확인
            elif len(jobs_list) == 0:
                has_more = False
            
            if not has_more:
                logger.info(f"✅ 페이지네이션 완료 (총 페이지: {page})")
                break
            
            page += 1
            
            # 페이지 간 적절한 휴식
            if page <= max_pages:
                rest_time = random.uniform(3, 8)
                logger.info(f"💤 다음 페이지 로딩 전 {rest_time:.1f}초 휴식...")
                time.sleep(rest_time)
        
        logger.info(f"✅ '{search_name}' 검색 완료: {len(search_jobs)}개 수집 (총 {total_found}개 발견)")
        return search_jobs
    
    def save_to_database(self, jobs: List[JobPosting]) -> int:
        """채용공고를 데이터베이스에 저장 (중복 처리)"""
        if not jobs:
            return 0
        
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            saved_count = 0
            updated_count = 0
            
            for job in jobs:
                # 기존 데이터 확인
                cursor.execute("SELECT id FROM job_postings WHERE position_id = ?", (job.position_id,))
                existing = cursor.fetchone()
                
                job_dict = asdict(job)
                
                if existing:
                    # 업데이트
                    update_fields = ", ".join([f"{k} = ?" for k in job_dict.keys()])
                    cursor.execute(
                        f"UPDATE job_postings SET {update_fields} WHERE position_id = ?",
                        list(job_dict.values()) + [job.position_id]
                    )
                    updated_count += 1
                else:
                    # 새로 삽입
                    placeholders = ", ".join(["?" for _ in job_dict])
                    fields = ", ".join(job_dict.keys())
                    cursor.execute(
                        f"INSERT INTO job_postings ({fields}) VALUES ({placeholders})",
                        list(job_dict.values())
                    )
                    saved_count += 1
            
            conn.commit()
            conn.close()
            
            logger.info(f"💾 DB 저장 완료: 신규 {saved_count}개, 업데이트 {updated_count}개")
            return saved_count + updated_count
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            return 0
    
    def save_crawling_log(self, search_type: str, total_found: int, successfully_crawled: int, 
                         failed_requests: int, start_time: str, end_time: str, duration: float):
        """크롤링 로그 저장"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO crawling_logs 
                (search_type, total_found, successfully_crawled, failed_requests, 
                 start_time, end_time, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (search_type, total_found, successfully_crawled, failed_requests,
                  start_time, end_time, duration))
            
            conn.commit()
            conn.close()
            logger.info("📝 크롤링 로그 저장 완료")
            
        except Exception as e:
            logger.error(f"❌ 로그 저장 실패: {e}")
    
    def export_to_csv(self, filename: str = None) -> str:
        """데이터베이스의 모든 데이터를 CSV로 내보내기"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"jumpit_jobs_{timestamp}.csv"
        
        try:
            conn = sqlite3.connect(self.db_name)
            
            # 채용공고 데이터 조회
            query = """
                SELECT position_id, title, company_name, location, career_level, 
                       employment_type, salary, tech_stacks, benefits, job_category,
                       description, requirements, preferred_qualifications,
                       company_description, company_size, company_industry,
                       posted_date, deadline, application_count, view_count,
                       bookmark_count, response_rate, tags, work_location_type,
                       experience_years, education_level, crawled_at, api_url
                FROM job_postings 
                ORDER BY crawled_at DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # CSV 저장 (한글 호환)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            logger.info(f"📊 CSV 내보내기 완료: {filename} ({len(df)}개 레코드)")
            return filename
            
        except Exception as e:
            logger.error(f"❌ CSV 내보내기 실패: {e}")
            return ""
    
    def get_database_stats(self) -> Dict:
        """데이터베이스 통계 조회"""
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # 전체 공고 수
            cursor.execute("SELECT COUNT(*) FROM job_postings")
            total_jobs = cursor.fetchone()[0]
            
            # 회사별 공고 수 (상위 10개)
            cursor.execute("""
                SELECT company_name, COUNT(*) as job_count 
                FROM job_postings 
                WHERE company_name != ''
                GROUP BY company_name 
                ORDER BY job_count DESC 
                LIMIT 10
            """)
            top_companies = cursor.fetchall()
            
            # 🔥 주요 기술 스택별 통계 (점핏 스타일)
            tech_stats = {}
            major_techs = [
                'Java', 'Python', 'JavaScript', 'React', 'Vue.js', 'Node.js',
                'Spring Boot', 'Django', 'PHP', 'C++', 'C#', 'AWS',
                'MySQL', 'Oracle', 'Docker', 'Kubernetes'
            ]
            
            for tech in major_techs:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM job_postings 
                    WHERE tech_stacks LIKE ?
                """, (f'%{tech}%',))
                count = cursor.fetchone()[0]
                if count > 0:
                    tech_stats[tech] = count
            
            # 직무 카테고리별 통계
            cursor.execute("""
                SELECT api_url, COUNT(*) as job_count 
                FROM job_postings 
                WHERE api_url != ''
                GROUP BY api_url 
                ORDER BY job_count DESC 
                LIMIT 10
            """)
            job_category_stats = cursor.fetchall()
            
            # 지역별 통계
            cursor.execute("""
                SELECT location, COUNT(*) as job_count 
                FROM job_postings 
                WHERE location != ''
                GROUP BY location 
                ORDER BY job_count DESC 
                LIMIT 10
            """)
            top_locations = cursor.fetchall()
            
            # 연봉 관련 통계
            cursor.execute("""
                SELECT COUNT(*) 
                FROM job_postings 
                WHERE salary != '' AND salary NOT LIKE '%협의%'
            """)
            salary_disclosed = cursor.fetchone()[0]
            
            # 경력별 통계
            cursor.execute("""
                SELECT career_level, COUNT(*) as count
                FROM job_postings 
                WHERE career_level != ''
                GROUP BY career_level 
                ORDER BY count DESC
            """)
            career_stats = cursor.fetchall()
            
            # 재택근무 가능 공고
            cursor.execute("""
                SELECT COUNT(*) 
                FROM job_postings 
                WHERE work_location_type LIKE '%재택%' OR work_location_type LIKE '%원격%'
                   OR tags LIKE '%재택%' OR tags LIKE '%원격%'
            """)
            remote_jobs = cursor.fetchone()[0]
            
            # 최근 크롤링 로그
            cursor.execute("""
                SELECT search_type, successfully_crawled, created_at
                FROM crawling_logs 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            recent_logs = cursor.fetchall()
            
            conn.close()
            
            return {
                "total_jobs": total_jobs,
                "top_companies": top_companies,
                "tech_stats": tech_stats,
                "job_category_stats": job_category_stats,
                "top_locations": top_locations,
                "salary_disclosed": salary_disclosed,
                "career_stats": career_stats,
                "remote_jobs": remote_jobs,
                "recent_logs": recent_logs
            }
            
        except Exception as e:
            logger.error(f"❌ 통계 조회 실패: {e}")
            return {}
    
    def run_full_crawling(self, max_pages_per_search: int = 5):
        """전체 크롤링 실행"""
        logger.info("🚀 점핏 전체 크롤링 시작!")
        logger.info(f"🎯 검색 유형: {len(self.search_params)}개")
        logger.info(f"📄 검색당 최대 페이지: {max_pages_per_search}")
        logger.info("=" * 60)
        
        overall_start = datetime.now()
        all_jobs = []
        total_searches = len(self.search_params)
        
        for idx, (search_name, params) in enumerate(self.search_params.items(), 1):
            logger.info(f"🔍 [{idx}/{total_searches}] '{search_name}' 검색 시작...")
            
            search_start = datetime.now()
            
            try:
                # 해당 검색 조건으로 크롤링
                search_jobs = self.crawl_search_type(search_name, params, max_pages_per_search)
                
                if search_jobs:
                    # 데이터베이스에 저장
                    saved_count = self.save_to_database(search_jobs)
                    all_jobs.extend(search_jobs)
                    
                    search_end = datetime.now()
                    duration = (search_end - search_start).total_seconds()
                    
                    # 크롤링 로그 저장
                    self.save_crawling_log(
                        search_name, len(search_jobs), saved_count, 0,
                        search_start.strftime('%Y-%m-%d %H:%M:%S'),
                        search_end.strftime('%Y-%m-%d %H:%M:%S'),
                        duration
                    )
                    
                    logger.info(f"✅ '{search_name}' 완료: {len(search_jobs)}개 수집, {saved_count}개 저장")
                else:
                    logger.warning(f"⚠️ '{search_name}' 검색 결과 없음")
                
            except Exception as e:
                logger.error(f"❌ '{search_name}' 크롤링 중 오류: {e}")
            
            # 검색 간 휴식 (마지막 검색이 아닌 경우)
            if idx < total_searches:
                rest_time = random.uniform(10, 20)
                logger.info(f"💤 다음 검색 전 {rest_time:.1f}초 휴식...")
                time.sleep(rest_time)
                logger.info("-" * 40)
        
        overall_end = datetime.now()
        total_duration = (overall_end - overall_start).total_seconds()
        
        # 최종 결과
        logger.info("🎉 === 전체 크롤링 완료! ===")
        logger.info(f"⏱️ 총 소요시간: {total_duration:.1f}초 ({total_duration/60:.1f}분)")
        logger.info(f"📊 총 수집 공고: {len(all_jobs)}개")
        
        # CSV 내보내기
        if all_jobs:
            csv_filename = self.export_to_csv()
            logger.info(f"📄 CSV 파일 생성: {csv_filename}")
        
        # 데이터베이스 통계
        stats = self.get_database_stats()
        if stats:
            logger.info(f"💾 DB 총 공고 수: {stats.get('total_jobs', 0)}개")
            
            logger.info("🏢 상위 채용 회사:")
            for company, count in stats.get('top_companies', [])[:5]:
                logger.info(f"   - {company}: {count}개")
            
            logger.info("📍 주요 지역:")
            for location, count in stats.get('top_locations', []):
                logger.info(f"   - {location}: {count}개")
            
            logger.info(f"🐍 Python 관련: {stats.get('python_jobs', 0)}개")
            logger.info(f"☕ Java 관련: {stats.get('java_jobs', 0)}개")
        
        logger.info("=" * 60)
        return len(all_jobs)

def main():
    """메인 실행 함수"""
    print("🚀 점핏(Jumpit) 채용공고 크롤러 v2.0")
    print("=" * 60)
    print("📋 주요 기능:")
    print("   - 점핏 실제 직무 분류 기반 크롤링")
    print("   - API 기반 안전한 크롤링")
    print("   - 서버 부하 최소화 (랜덤 지연)")
    print("   - SQLite DB 자동 저장")
    print("   - CSV 파일 내보내기")
    print("   - 상세 로깅 시스템")
    print("   - 중복 데이터 처리")
    print("=" * 60)
    print("🎯 주요 검색 대상:")
    print("   💻 개발: 서버/백엔드, 프론트엔드, 풀스택, 모바일")
    print("   🔧 인프라: DevOps, DBA, 시스템엔지니어")
    print("   📊 데이터: 빅데이터, AI/ML 엔지니어")
    print("   🎨 기타: QA, PM, 보안, 블록체인")
    print("   🏠 특별: 재택근무, 신입환영, 고연봉")
    print("   💻 기술스택: Java, Python, React, AWS 등")
    print("=" * 60)
    
    try:
        # 크롤러 초기화
        crawler = JumpitCrawler()
        
        # 사용자 선택
        print("\n실행 옵션을 선택하세요:")
        print("1. 전체 크롤링 (모든 검색 조건)")
        print("2. 특정 검색만 실행")
        print("3. 데이터베이스 통계만 보기")
        print("4. CSV 내보내기만 실행")
        
        choice = input("\n선택 (1-4): ").strip()
        
        if choice == "1":
            # 전체 크롤링
            pages_input = input("검색당 최대 페이지 수 (기본값 5): ").strip()
            max_pages = int(pages_input) if pages_input.isdigit() else 5
            
            print(f"\n🚀 전체 크롤링 시작 (페이지당 최대 {max_pages}개)...")
            total_jobs = crawler.run_full_crawling(max_pages)
            
            if total_jobs > 0:
                print(f"\n🎉 크롤링 성공! 총 {total_jobs}개 채용공고 수집")
                print("📁 파일 위치:")
                print(f"   - 데이터베이스: {crawler.db_name}")
                print(f"   - 로그 파일: jumpit_crawler.log")
                
                # 자동으로 CSV도 생성
                csv_file = crawler.export_to_csv()
                if csv_file:
                    print(f"   - CSV 파일: {csv_file}")
            else:
                print("❌ 수집된 데이터가 없습니다.")
        
        elif choice == "2":
            # 특정 검색 선택
            print("\n검색 조건을 선택하세요:")
            search_list = list(crawler.search_params.keys())
            for i, search_name in enumerate(search_list, 1):
                print(f"{i}. {search_name}")
            
            search_choice = input(f"\n선택 (1-{len(search_list)}): ").strip()
            
            if search_choice.isdigit() and 1 <= int(search_choice) <= len(search_list):
                selected_search = search_list[int(search_choice) - 1]
                selected_params = crawler.search_params[selected_search]
                
                pages_input = input("최대 페이지 수 (기본값 10): ").strip()
                max_pages = int(pages_input) if pages_input.isdigit() else 10
                
                print(f"\n🎯 '{selected_search}' 검색 시작...")
                
                search_start = datetime.now()
                jobs = crawler.crawl_search_type(selected_search, selected_params, max_pages)
                
                if jobs:
                    saved_count = crawler.save_to_database(jobs)
                    search_end = datetime.now()
                    duration = (search_end - search_start).total_seconds()
                    
                    crawler.save_crawling_log(
                        selected_search, len(jobs), saved_count, 0,
                        search_start.strftime('%Y-%m-%d %H:%M:%S'),
                        search_end.strftime('%Y-%m-%d %H:%M:%S'),
                        duration
                    )
                    
                    print(f"✅ 완료! {len(jobs)}개 수집, {saved_count}개 저장")
                    
                    # CSV 내보내기 제안
                    export_csv = input("CSV 파일로 내보내시겠습니까? (y/n): ").strip().lower()
                    if export_csv == 'y':
                        csv_file = crawler.export_to_csv()
                        if csv_file:
                            print(f"📄 CSV 파일 생성: {csv_file}")
                else:
                    print("❌ 수집된 데이터가 없습니다.")
            else:
                print("❌ 잘못된 선택입니다.")
        
        elif choice == "3":
            # 데이터베이스 통계
            print("\n📊 데이터베이스 통계 조회 중...")
            stats = crawler.get_database_stats()
            
            if stats:
                print(f"\n💾 전체 채용공고: {stats.get('total_jobs', 0)}개")
                
                print("\n🏢 상위 채용 회사:")
                for company, count in stats.get('top_companies', [])[:7]:
                    print(f"   {company}: {count}개")
                
                print("\n📍 지역별 분포:")
                for location, count in stats.get('top_locations', [])[:7]:
                    print(f"   {location}: {count}개")
                
                print("\n💻 주요 기술 스택:")
                tech_stats = stats.get('tech_stats', {})
                # 기술스택을 사용 빈도순으로 정렬
                sorted_techs = sorted(tech_stats.items(), key=lambda x: x[1], reverse=True)
                for tech, count in sorted_techs[:10]:  # 상위 10개만
                    print(f"   {tech}: {count}개")
                
                print(f"\n🏠 재택근무 가능 공고: {stats.get('remote_jobs', 0)}개")
                print(f"💰 연봉 공개 공고: {stats.get('salary_disclosed', 0)}개")
                
                print("\n👨‍💼 경력별 분포:")
                for career, count in stats.get('career_stats', [])[:5]:
                    print(f"   {career}: {count}개")
                
                print("\n🎯 직무별 수집 현황:")
                for job_type, count in stats.get('job_category_stats', [])[:8]:
                    # API URL에서 의미있는 이름 추출
                    display_name = job_type.replace('_', ' ').title() if job_type else '기타'
                    print(f"   {display_name}: {count}개")
                
                print("\n📝 최근 크롤링 로그:")
                for search_type, crawled, created_at in stats.get('recent_logs', [])[:7]:
                    print(f"   {created_at}: {search_type} - {crawled}개")
            else:
                print("❌ 통계 조회 실패")
        
        elif choice == "4":
            # CSV 내보내기만
            print("\n📄 CSV 내보내기 중...")
            csv_file = crawler.export_to_csv()
            if csv_file:
                print(f"✅ CSV 파일 생성 완료: {csv_file}")
                
                # 파일 크기 확인
                try:
                    file_size = os.path.getsize(csv_file)
                    print(f"📏 파일 크기: {file_size / 1024:.1f} KB")
                except:
                    pass
            else:
                print("❌ CSV 내보내기 실패")
        
        else:
            print("❌ 잘못된 선택입니다.")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류 발생: {e}")
        logger.error(f"메인 함수 오류: {e}")
    
    print("\n👋 프로그램을 종료합니다.")

def run_quick_test():
    """빠른 테스트 함수"""
    print("🧪 점핏 API 연결 테스트...")
    
    crawler = JumpitCrawler()
    
    # 재택근무 공고 1페이지만 테스트
    test_jobs = crawler.crawl_search_type("재택근무_테스트", {"tag": "WORK_AT_HOME_COMPANY"}, max_pages=1)
    
    if test_jobs:
        print(f"✅ 테스트 성공! {len(test_jobs)}개 공고 발견")
        
        # 첫 번째 공고 정보 출력
        if test_jobs:
            job = test_jobs[0]
            print(f"\n📋 샘플 공고:")
            print(f"   제목: {job.title}")
            print(f"   회사: {job.company_name}")
            print(f"   위치: {job.location}")
            print(f"   기술스택: {job.tech_stacks}")
            
        # 간단 저장 테스트
        saved = crawler.save_to_database(test_jobs[:3])  # 처음 3개만
        print(f"💾 DB 저장 테스트: {saved}개 저장됨")
        
    else:
        print("❌ 테스트 실패 - API 연결 문제 가능성")

if __name__ == "__main__":
    # 사용법 예시
    print("사용 옵션:")
    print("1. 전체 실행: python jumpit_crawler.py")
    print("2. 빠른 테스트: python -c 'from jumpit_crawler import run_quick_test; run_quick_test()'")
    print()
    
    main()