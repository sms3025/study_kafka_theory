# 01. 이벤트와 Kafka의 구조

주문 한 건이 여러 서비스에 도착하는 길을 따라가며 Kafka의 역할을 이해합니다.

먼저 [대응 이론 HTML](../../이론/초급/01.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 요청·응답과 이벤트 전달을 구분한다
- Producer → Broker → Consumer의 책임을 설명한다
- 토픽을 만들고 저장된 메시지를 읽는다

## 실행과 관찰

### 1. 환경 시작

```powershell
.\실습\study.ps1 up
```

예상 결과: Kafka와 PostgreSQL이 healthy가 됩니다. 첫 실행은 이미지 다운로드 시간이 필요합니다.

### 2. 코드 빌드

```powershell
.\실습\study.ps1 build
```

예상 결과: 실습/target/kafka-labs.jar가 생성됩니다.

### 3. 토픽 생성

```powershell
.\실습\study.ps1 init
```

예상 결과: orders 등 5개 토픽과 각 파티션의 Leader/Replicas/ISR가 출력됩니다.

### 4. 첫 이벤트

```powershell
.\실습\study.ps1 run producer 3 orders day01
```

예상 결과: SENT 3줄이 출력됩니다.

### 5. 읽기

```powershell
.\실습\study.ps1 run consumer day01-reader 3 orders
```

예상 결과: RECEIVED 3줄과 CONSUMED 3이 출력됩니다.

## 더 실험하기

- 새 그룹 day01-other로 같은 3건을 다시 읽어 보세요. 읽었다고 레코드가 삭제되지는 않습니다.
- 동일한 그룹으로 재실행하면 커밋한 위치에서 이어 읽습니다. 새 메시지가 없으면 25초 뒤 종료합니다.

## 주의할 경계

실습 명령은 현재 저장된 데이터에 이어서 작동합니다. 처음부터 관찰하고 싶으면 새 group 이름을 쓰세요. 기존 그룹에 earliest를 설정하는 것만으로 처음부터 돌아가지는 않습니다.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/TopicAdmin.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 요청·응답과 이벤트 전달을 구분한다
- [ ] Producer → Broker → Consumer의 책임을 설명한다
- [ ] 토픽을 만들고 저장된 메시지를 읽는다

이해도 확인: Producer가 전송 성공 응답을 받았습니다. 확실히 말할 수 있는 것은?

<details><summary>답 확인</summary>

설정된 ack 조건에 따라 Kafka 기록이 확인되었다 — Kafka 기록 확인과 Consumer의 업무 완료는 서로 다른 시점입니다.

</details>
