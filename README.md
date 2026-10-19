# Kafka Field Guide — 원리에서 실행까지

Kafka 입문자를 위한 **초급 → 중급 → 고급, 12개 단원**의 로컬 학습 자료입니다.
이론의 개념·SVG 흐름도·단계별 재생·코드 해설을 먼저 읽고, 같은 번호의 실습 안내에 따라 Java 코드를 실행합니다.

**시작:** [이론 학습 페이지](이론/index.html)를 브라우저로 열거나, 저장소의 `index.html`을 더블 클릭하세요.
HTML은 외부 CDN·웹 폰트·서버 없이 열립니다. 인터넷은 Docker 이미지와 Maven 의존성을 처음 받을 때 필요합니다.

## 구성

```text
이론/
  index.html              전체 로드맵, 검색, 학습 진행 기록
  초급/01.html ~ 04.html   이벤트, 파티션, 그룹, Java API
  중급/05.html ~ 08.html   커밋, 멱등 처리, 재시도/DLT, 복제
  고급/09.html ~ 12.html   트랜잭션, Outbox, Streams, 운영
  assets/                 공통 CSS/JS, 12개 SVG 데이터 흐름 이미지
실습/
  README.md               실행 환경과 명령 설명
  초급/ 중급/ 고급/        번호별 안내와 실제 Java 소스
  공통/                   설정, 이벤트 모델, PostgreSQL 스키마
  검증/                   실제 Kafka/PostgreSQL 통합 검사
  compose.yaml            단일 / 3노드 KRaft 환경
  pom.xml                 Java 21, Kafka 4.3.1, PostgreSQL JDBC
  study.ps1               Windows PowerShell 실행 도우미
  최종-프로젝트.md          전체 흐름과 복구 실험
검증/                     정적 자료 검사와 검증 기록
tools/                    HTML/실습 안내 재생성 도구
```

## 권장 순서

| 단계 | 단원 | 직접 확인할 결과 |
|---|---|---|
| 초급 | 01~04 | 저장·구독, key별 partition, 그룹 분담, Java 송수신 |
| 중급 | 05~08 | 커밋 실패 구간, DB 멱등성, DLT 기록, Leader 교체 |
| 고급 | 09~12 | 트랜잭션 격리, Outbox 재발행, 상태 집계, Lag·스키마 |

각 단원 첫 학습에 약 35~75분, 총 약 11.5시간을 예상합니다. 반복 실험·프로젝트·설치와 디버깅 시간은 별도입니다.
주 2단원씩 6주 진행하면서 `예상 → 실행 → 결과 → 이유`를 기록하는 것을 권장합니다.

## 실습 시작

Docker Desktop을 Linux containers 모드로 실행하고, 이 저장소 루트의 PowerShell에서 실행합니다.
호스트 Java/Maven 설치는 필요 없습니다. **첫 다운로드에는 수 GB의 여유 공간과 인터넷이 필요**합니다.

```powershell
.\실습\study.ps1 up
.\실습\study.ps1 build
.\실습\study.ps1 init
.\실습\study.ps1 run producer 3 orders first-run
.\실습\study.ps1 run consumer first-reader 3 orders
```

PowerShell 실행 정책이 스크립트를 차단하면 정책을 영구 변경하지 않고 해당 실행만 허용할 수 있습니다.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\실습\study.ps1 up
```

실습 후 종료하면 컨테이너는 제거하고 로그·DB 볼륨은 보존합니다.

```powershell
.\실습\study.ps1 down
```

## 자료의 특성

- 이론의 Java 발췌는 실제 예제 소스에서 생성합니다. 전체 파일도 연결되어 있습니다.
- 도식과 시뮬레이터는 설명용 모델이며 실제 Kafka의 타이밍·파티션 할당을 그대로 재현하지 않습니다.
- 브라우저 학습 진행은 해당 브라우저의 localStorage에만 저장합니다. `file://`와 HTTP로 연 페이지의 기록은 다를 수 있습니다.
- 현업에서 사용하는 API와 패턴을 학습 가능한 크기로 구현했습니다. 보안·부하 제어·운영 자동화를 포함한 완성 서비스는 아닙니다.
- 주제별 공식 출처는 각 이론 페이지 하단에 있습니다.

## 검증과 유지보수

```powershell
python .\검증\check_materials.py
.\실습\study.ps1 run verify
# 클러스터를 시작·init한 뒤에는 다음 명령으로 동일 보장을 확인합니다.
.\실습\study.ps1 run -Cluster verify
```

실행 범위와 관찰 결과는 [검증 안내](검증/README.md)를 확인하세요.

이론과 실습 안내의 원본은 `tools/course_content.py`입니다. Java의 `region` 발췌 구간을 수정하거나 내용을 수정했다면 재생성합니다.

```powershell
python .\tools\build_theory.py
```
