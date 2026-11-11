# pkstt-backend

Backend API w Go agregujący rozkłady jazdy **ZTM / MPK Rzeszów** oraz **PKS Rzeszów (ZGPKS / MKS)** na podstawie otwartych danych z [kmrzeszow.live/otwartedane](https://kmrzeszow.live/otwartedane/).

## Architektura

- **Statyczne GTFS (PostgreSQL 17):**
  - Okresowa synchronizacja paczek ZTM i PKS (domyślnie co `6h`) z warunkowym HTTP (`ETag`, `Last-Modified`, `SHA-256`).
  - Separacja identyfikatorów na poziomie bazy (`feed = 'ztm' | 'pks'`) i API (`ztm:<id>`, `pks:<id>`), co eliminuje kolizje między przewoźnikami.
  - Automatyczne łączenie przystanków obu przewoźników w grupy przystankowe (`stop_groups`) oraz słupki/warianty (`bays`) wraz z przykładowymi kierunkami.
  - Obsługa czasów GTFS `>= 24:00:00` (kursy nocne po północy) i zmian czasu w strefie `Europe/Warsaw`.
- **GTFS-Realtime (cache w pamięci Go):**
  - Odpytywanie plików `gtfsrt.pb` dla ZTM i PKS (domyślnie co `10s`) z obsługą `304 Not Modified`.
  - Wzbogacanie pozycji GPS o numer linii, brygadę, kierunek oraz mapowanie pojazdu do kursu (`VehicleByTrip` / `has_live_location`).
  - Propagacja opóźnień z `TripUpdate` na kolejne przystanki danego kursu (`stop_sequence`).
- **Bezpieczeństwo i publikacja:**
  - API wyłącznie do odczytu (`GET`, `HEAD`, `OPTIONS`) z walidacją parametrów i nagłówkami bezpieczeństwa.
  - Rate limiting per IP (token bucket) z obsługą `CF-Connecting-IP` wyłącznie dla zaufanych lokalnych proxy (`TRUSTED_PROXY=private`).
  - Kontener uruchamiany jako użytkownik nieuprzywilejowany (`10001`), z systemem plików `read_only`, `cap_drop: [ALL]` oraz bindowaniem domyślnie na `127.0.0.1:8080` pod **Cloudflare Tunnel**.

## Endpointy API

| Metoda i ścieżka | Opis |
|---|---|
| `GET /healthz` | Stan API, połączenia z PostgreSQL oraz feedów live |
| `GET /v1/meta` | Wersje i daty aktualizacji feedów statycznych (`static_feeds`) oraz status feedów live (`realtime_feeds`) |
| `GET /v1/stop-groups` | Lista lub wyszukiwanie grup przystankowych (`?q=`, `?lat=&lon=&radius=`, `?operator=ztm\|pks&limit=`) |
| `GET /v1/stop-groups/{groupID}` | Szczegóły grupy, lista słupków/wariantów z przykładowymi liniami i kierunkami |
| `GET /v1/stop-groups/{groupID}/departures` | Tablica najbliższych odjazdów ZTM + PKS z danymi live (`?window=`, `?limit=`, `?operator=`, `?route=`, `?stop_ids=`, `?at=`) |
| `GET /v1/stops/{stopID}/departures` | Tablica odjazdów dla konkretnego słupka (np. `ztm:65`, `pks:199`) |
| `GET /v1/routes` | Lista wszystkich linii ZTM i PKS (`?operator=&q=`) |
| `GET /v1/routes/{routeID}` | Szczegóły linii i warianty tras (np. `ztm:43`, `pks:1`) |
| `GET /v1/routes/{routeID}/timetable` | Tabliczka przystankowa linii z podziałem na typy dni i legendą wariantów (`?direction_id=&stop_id=`) |
| `GET /v1/trips/{tripID}` | Przebieg kursu z czasami planowymi i live, pozycją pojazdu oraz kształtem trasy (`?service_date=&include_shape=true`) |
| `GET /v1/shapes/{shapeID}` | Punkty kształtu trasy (`[[lat, lon], ...]`) z obsługą `ETag` |
| `GET /v1/vehicles` | Aktywne pojazdy na żywo (`?operator=&route=&bbox=minLat,minLon,maxLat,maxLon`), obsługuje `If-None-Match` |
| `GET /v1/alerts` | Aktywne komunikaty GTFS-RT (`?operator=`) |
| `GET /v1/fares` | Taryfy biletowe z `fare_products.txt` |

## Zmienne środowiskowe (`.env`)

Przykładowa konfiguracja znajduje się w [deploy/.env.example](deploy/.env.example):

- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` – dane połączenia z PostgreSQL (wymagane)
- `API_PORT` – lokalny port HTTP bindujący się na `127.0.0.1` (domyślnie `8080`)
- `STATIC_SYNC_INTERVAL`, `RT_POLL_INTERVAL` – interwały odświeżania GTFS statycznego (`6h`) i GTFS-RT (`10s`)
- `TRUSTED_PROXY`, `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`, `CORS_ALLOWED_ORIGINS` – ustawienia proxy i zabezpieczeń HTTP
- `CLOUDFLARE_TUNNEL_TOKEN` – opcjonalny token tunelu dla profilu `tunnel`

## Uruchomienie przez Docker Compose

1. Sklonuj repozytorium na serwer z zainstalowanym Dockerem i wtyczką Compose (opcjonalnie na Debianie można użyć pomocniczego skryptu `deploy/setup-vm.sh`):
   ```bash
   git clone https://github.com/maciejdbrznsk/pkstt-backend.git
   cd pkstt-backend
   ```
2. Przygotuj plik `.env`:
   ```bash
   cp deploy/.env.example .env
   ```
   Ustaw silne hasło `POSTGRES_PASSWORD` oraz – jeśli publikujesz API przez Cloudflare Zero Trust – `CLOUDFLARE_TUNNEL_TOKEN`.
3. Uruchom stos lokalnie (`127.0.0.1:8080`) lub od razu z profilem tunelu Cloudflare:
   ```bash
   # Bez tunelu (np. za własnym reverse proxy):
   docker compose up -d --build

   # Z wbudowanym kontenerem cloudflared:
   docker compose --profile tunnel up -d --build
   ```
   W panelu Cloudflare Tunnel ustaw **Public Hostname** na `HTTP -> api:8080`.
4. Sprawdź działanie:
   ```bash
   curl -s http://127.0.0.1:8080/healthz
   curl -s http://127.0.0.1:8080/v1/meta
   ```

## Testy lokalne

Wymagane **Go 1.26+**:

```bash
go test -v ./...
```

## Źródła danych

Dane rozkładowe (GTFS) i pozycje/opóźnienia w czasie rzeczywistym (GTFS-RT) pochodzą z publicznych otwartych zbiorów udostępnianych pod adresem [kmrzeszow.live/otwartedane](https://kmrzeszow.live/otwartedane/).
