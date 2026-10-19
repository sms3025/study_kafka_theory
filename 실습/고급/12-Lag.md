# 12. Lag · 스키마 · 운영 점검

지연·장애·데이터 호환성을 관찰하고 최종 파이프라인을 검증합니다.

먼저 [대응 이론 HTML](../../이론/고급/12.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- Lag을 처리 시간과 구분해 해석한다
- 추가 필드와 호환 불가 변경을 구분한다
- 장애·중복·복구를 포함한 완료 기준을 만든다

## 실행과 관찰

### 1. 느린 Consumer: 터미널 A

```powershell
.\실습\study.ps1 run consumer lag-day12 0 orders 1000
```

예상 결과: 레코드당 1초를 기다립니다. 이 Consumer는 출력 대역 뒤에 커밋합니다.

### 2. 입력: 터미널 B

```powershell
.\실습\study.ps1 run producer 60 orders day12
```

예상 결과: Consumer 처리보다 빠르게 입력을 만듭니다.

### 3. 관측: 터미널 C

```powershell
.\실습\study.ps1 run inspect lag-day12
```

예상 결과: next, end, lag과 Leader/ISR 정보가 출력됩니다. 주기적으로 다시 실행해 비교하세요.

### 4. 스키마 확인

```powershell
.\실습\study.ps1 run schema
```

예상 결과: OPTIONAL_FIELD_OK와 UNSUPPORTED_VERSION_REJECTED가 출력됩니다.

### 5. 실제 통합 검사

```powershell
.\실습\study.ps1 run verify
```

예상 결과: 송수신, key 분배, 복구 위치, 트랜잭션 격리, DB 멱등성, Outbox 롤백이 통과합니다.

## 더 실험하기

- 검증 명령의 PASS가 어떤 보장 범위를 확인하고, 어떤 부하·보안·성능을 확인하지 않는지 적어 보세요.
- 최종 프로젝트 안내에 따라 Order API 대역 → Outbox → Kafka → 집계 Consumer를 실행하고 장애 전후 결과를 기록하세요.

## 주의할 경계

운영 환경은 이 학습용 Compose를 그대로 배포하는 대상이 아닙니다. 이 강의의 통합 검사는 기능적 보장을 확인하며 운영 부하·장시간 안정성·보안 검증을 대신하지 않습니다.

## 읽을 소스

- [발췌한 전체 소스](../고급/src/main/java/study/kafka/OperationsLab.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] Lag을 처리 시간과 구분해 해석한다
- [ ] 추가 필드와 호환 불가 변경을 구분한다
- [ ] 장애·중복·복구를 포함한 완료 기준을 만든다

이해도 확인: Consumer Lag=0이면 항상 업무 처리 지연도 0초인가요?

<details><summary>답 확인</summary>

아니다. 커밋 위치와 업무 시간은 별도 지표다 — 커밋 위치와 실제 업무 완료·시간 지연은 같은 지표가 아닙니다. 커밋 시점도 함께 확인해야 합니다.

</details>
