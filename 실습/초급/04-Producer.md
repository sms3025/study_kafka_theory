# 04. Producer · Consumer API

JSON 이벤트를 만들고, 전송 결과와 소비 위치를 코드에서 다룹니다.

먼저 [대응 이론 HTML](../../이론/초급/04.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 직렬화와 역직렬화를 설명한다
- 비동기 send의 결과를 확인한다
- 설정과 업무 코드를 구분한다

## 실행과 관찰

### 1. 실행 옵션 확인

```powershell
.\실습\study.ps1 run help
```

예상 결과: 전체 명령과 위치 인자가 출력됩니다.

### 2. 같은 eventId로 발행

```powershell
.\실습\study.ps1 run producer 3 orders day04
```

예상 결과: SENT 3건입니다. 같은 명령을 한 번 더 실행하세요.

### 3. 중복을 관찰

```powershell
.\실습\study.ps1 run consumer java-demo 0 orders
```

예상 결과: day04-0 등 같은 eventId가 서로 다른 offset으로 두 번 존재하는지 확인합니다.

### 4. 비동기 결과와 대기열 제한

```powershell
.\실습\study.ps1 run producer-async 12 orders day04-async
```

예상 결과: ASYNC_ACK 12줄이 출력됩니다. 파티션 간 callback 출력 순서는 다를 수 있습니다.

### 5. 실제 연결 검증

```powershell
.\실습\study.ps1 run verify
```

예상 결과: 전용 토픽을 만든 뒤 송수신·커밋·트랜잭션·DB를 검증하고 ALL_CHECKS_PASSED를 출력합니다.

## 더 실험하기

- OrderProducer에서 amount를 바꾸고 build 후 다시 실행하세요. 빌드 전후 결과가 어떻게 다른지 확인하세요.
- 한글 주문 정보를 JSON의 선택 필드로 추가하고 schema 명령의 호환성 실습과 연결해 보세요.

## 주의할 경계

제공 코드는 운영에서 사용하는 API와 패턴을 재현한 학습용 구현입니다. 인증, 커넥션 풀, 관측, 부하 제어, 개인정보 정책을 갖춘 완성 서비스라는 의미는 아닙니다.

## 읽을 소스

- [발췌한 전체 소스](../공통/src/main/java/study/kafka/Settings.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 직렬화와 역직렬화를 설명한다
- [ ] 비동기 send의 결과를 확인한다
- [ ] 설정과 업무 코드를 구분한다

이해도 확인: 멱등 Producer를 사용하면서 같은 이벤트를 새 send 호출로 두 번 보내면?

<details><summary>답 확인</summary>

두 레코드가 기록될 수 있다 — Producer 멱등성은 애플리케이션의 업무 eventId 중복을 판별하지 않습니다.

</details>
