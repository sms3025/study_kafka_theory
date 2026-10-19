# 02. Partition · Key · Offset

같은 주문의 이벤트를 묶고, 파티션 안에서 순서와 위치를 관찰합니다.

먼저 [대응 이론 HTML](../../이론/초급/02.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 파티션별 순서 보장을 설명한다
- key와 partition의 관계를 확인한다
- offset을 토픽 전체의 번호로 오해하지 않는다

## 실행과 관찰

### 1. 주문 이벤트 전송

```powershell
.\실습\study.ps1 run producer 12 orders day02
```

예상 결과: 3가지 key에 대해 SENT 로그 12줄이 출력됩니다.

### 2. 파티션 정보

```powershell
.\실습\study.ps1 topics
```

예상 결과: orders에 P0~P2가 있고 각 파티션의 Leader가 표시됩니다.

### 3. 읽기

```powershell
.\실습\study.ps1 run consumer day02-reader 0 orders
```

예상 결과: 그룹이 처음이라면 기존 기록도 읽습니다. key별 partition과 offset을 기록하세요.

## 더 실험하기

- 전송 로그에서 order-0에 해당하는 줄만 골라 partition이 같은지 확인하세요.
- key 3개가 반드시 서로 다른 파티션에 들어가는지 검증하세요. 충돌은 정상입니다.

## 주의할 경계

그림의 order-A → P0 배치는 설명용입니다. 특정 문자열이 특정 파티션으로 간다고 하드코딩하지 않습니다. 파티션 수를 바꿔 보는 실험은 새 토픽으로 분리하는 편이 비교하기 쉽습니다.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/OrderProducer.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 파티션별 순서 보장을 설명한다
- [ ] key와 partition의 관계를 확인한다
- [ ] offset을 토픽 전체의 번호로 오해하지 않는다

이해도 확인: Kafka가 기본적으로 보장하는 순서의 범위는?

<details><summary>답 확인</summary>

각 파티션 내부 — 하나의 토픽에 여러 파티션이 있으면 파티션 간 전역 순서는 보장되지 않습니다.

</details>
