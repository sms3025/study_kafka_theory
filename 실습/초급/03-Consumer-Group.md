# 03. Consumer Group · Rebalance

같은 그룹의 분담과 다른 그룹의 독립 구독을 구분합니다.

먼저 [대응 이론 HTML](../../이론/초급/03.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 동일 그룹과 다른 그룹의 차이를 관찰한다
- 파티션 수와 Consumer 병렬성의 관계를 설명한다
- 리밸런싱 시 할당 변화를 읽는다

## 실행과 관찰

### 1. 터미널 A

```powershell
.\실습\study.ps1 run consumer notify-demo 0 orders 500
```

예상 결과: Consumer가 실행됩니다. 기록이 없으면 25초 뒤 종료하므로 이어지는 명령을 바로 실행하세요.

### 2. 터미널 B

```powershell
.\실습\study.ps1 run consumer notify-demo 0 orders 500
```

예상 결과: 동일 그룹으로 가입하고 ASSIGNED 로그가 바뀝니다.

### 3. 터미널 C

```powershell
.\실습\study.ps1 run consumer analytics-demo 0 orders
```

예상 결과: 다른 그룹은 기록 전체를 독립적으로 읽습니다.

### 4. 터미널 D

```powershell
.\실습\study.ps1 run producer 60 orders day03
```

예상 결과: notify 그룹 두 Consumer의 처리 합계와 analytics 그룹의 처리량을 비교합니다.

## 더 실험하기

- A와 B의 ASSIGNED 파티션이 같은 시점에 겹치는지 확인하세요.
- 동일 그룹 Consumer를 4개 실행해 할당이 비어 있는 Consumer를 찾아보세요.
- 실행 중 하나를 종료하고 남은 Consumer의 REVOKED/ASSIGNED 로그를 확인하세요.

## 주의할 경계

이 강의는 일반 Consumer Group을 다룹니다. 레코드 단위 분배를 제공하는 Share Group은 별도 모델입니다. 그룹의 멤버 수와 파티션 수 관계를 모든 Kafka 소비 방식에 일반화하지 마세요.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/OrderConsumer.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 동일 그룹과 다른 그룹의 차이를 관찰한다
- [ ] 파티션 수와 Consumer 병렬성의 관계를 설명한다
- [ ] 리밸런싱 시 할당 변화를 읽는다

이해도 확인: 알림과 통계가 각각 모든 이벤트를 받아야 할 때 group.id는?

<details><summary>답 확인</summary>

서로 달라야 한다 — 서로 다른 그룹은 같은 토픽을 독립적으로 구독하고 각자 offset을 관리합니다.

</details>
