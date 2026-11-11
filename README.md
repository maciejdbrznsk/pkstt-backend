# pkstt-backend

Backend API w Go łączący rozkłady jazdy **ZTM Rzeszów** i **PKS Rzeszów (ZGPKS)** na podstawie otwartych danych z [kmrzeszow.live/otwartedane](https://kmrzeszow.live/otwartedane/).

## Architektura

- **Statyczne GTFS (PostgreSQL 17):**
  - ZTM (`https://kmrzeszow.live/gtfs/latest.zip`) i PKS (`https://kmrzeszow.live/gtfs-pks/latest.zip`) sprawdzane co `6h` (4× dziennie) z warunkowym HTTP (`ETag` / `Last-Modified` / `SHA-256`).
  - Przestrzenie ID są rozdzielone na poziomie bazy (`feed = 'ztm' | 'pks'`) oraz API (`ztm:<id>`, `pks:<id>`), dzięki czemu nie dochodzi do kolizji identyfikatorów.
  - Przystanki ZTM i PKS są automatycznie łączone w logiczne **grupy przystankowe** (`stop_groups`, np. `lubelska-szpital` zawiera jednocześnie przystanki ZTM i PKS na obu kierunkach).
  - Obsługa czasów GTFS `>= 24:00:00` (kursy nocne przechodzące przez północ) oraz przejść czasu letniego/zimowego w strefie `Europe/Warsaw`.
- **GTFS-Realtime (cache w pamięci Go):**
  - Pobieranie `gtfsrt.pb` dla ZTM i PKS co `10s` z użyciem `If-None-Match` / `If-Modified-Since` (`304 Not Modified` przy braku zmian).
  - Pozycje pojazdów są wzbogacane o numer linii, kolor, kierunek i brygadę z metadanych kursów.
  - Opóźnienia z `TripUpdate` są propagowane do kolejnych przystanków danego kursu (`stop_sequence`).
- **Bezpieczeństwo:**
  - API wyłącznie do odczytu (`GET`, `HEAD`, `OPTIONS`), z walidacją parametrów i nagłówkami bezpieczeństwa.
  - Rate limiting per-IP (token bucket) z obsługą `CF-Connecting-IP` **tylko** od zaufanych lokalnych proxy (`TRUSTED_PROXY=private`).
  - Kontener Docker działa jako użytkownik nieuprzywilejowany (`10001`), z systemem plików `read_only`, `cap_drop: [ALL]` oraz wystawionym portem wyłącznie na `127.0.0.1:8080` pod **Cloudflare Tunnel**.

## Endpointy API (`v1`)

| Metoda i ścieżka | Opis |
|---|---|
| `GET /healthz` | Stan aplikacji, połączenia z PostgreSQL oraz feedów live |
| `GET /v1/meta` | Wersje rozkładów statycznych, czasy synchronizacji i statystyki |
| `GET /v1/stop-groups` | Wyszukiwanie (`?q=lubelska`) lub najbliższe przystanki (`?lat=50.049&lon=22.001&radius=800`), opcjonalnie `?operator=ztm\|pks&limit=20` |
| `GET /v1/stop-groups/{groupID}` | Szczegóły węzła przystankowego, perony ZTM+PKS oraz obsługiwane linie |
| `GET /v1/stop-groups/{groupID}/departures` | Połączona tablica odjazdów ZTM + PKS z opóźnieniami live (`?window=120&limit=40&operator=&route=&at=`) |
| `GET /v1/stops/{stopID}/departures` | Tablica odjazdów dla pojedynczego słupka (np. `ztm:65`, `pks:199`) |
| `GET /v1/routes` | Lista wszystkich linii ZTM i PKS (`?operator=&q=`) |
| `GET /v1/routes/{routeID}` | Szczegóły linii i warianty tras z listą przystanków (np. `ztm:43`, `pks:1`) |
| `GET /v1/trips/{tripID}` | Przebieg kursu z czasami rozkładowymi i live, pozycją pojazdu oraz opcjonalnym kształtem (`?include_shape=true`) |
| `GET /v1/shapes/{shapeID}` | Współrzędne przebiegu trasy (`[[lat, lon], ...]`) z cache `ETag` |
| `GET /v1/vehicles` | Pozycje autobusów na żywo (`?operator=&route=&bbox=minLat,minLon,maxLat,maxLon`), obsługuje `If-None-Match` |
| `GET /v1/alerts` | Aktywne komunikaty GTFS-RT (`?operator=`) |
| `GET /v1/fares` | Lista biletów i taryf zaimportowanych z `fare_products.txt` |

## Testy lokalne

```bash
go test -v ./...
```

## Wdrożenie na Debian 13 (`172.16.8.108`) przez Docker + Cloudflare Tunnel

1. Skopiuj repozytorium na VM (np. `git clone` lub `scp`):
   ```bash
   ssh pkstt@172.16.8.108
   git clone https://github.com/maciejdbrznsk/pkstt-backend.git
   cd pkstt-backend
   ```
2. Przygotuj VM (UFW tylko SSH, fail2ban, Docker Engine + Compose):
   ```bash
   chmod +x deploy/setup-vm.sh
   ./deploy/setup-vm.sh
   ```
   *(Jeśli skrypt dodał użytkownika do grupy `docker`, przeloguj się przez SSH).*
3. Utwórz tunel w panelu **Cloudflare Zero Trust → Networks → Tunnels**:
   - Skopiuj token tunelu (`CLOUDFLARE_TUNNEL_TOKEN`).
   - Dodaj **Public Hostname** (np. `api.twojadomena.pl`) kierujący na:
     - **Type:** `HTTP`
     - **URL:** `api:8080`
4. Utwórz plik `.env`:
   ```bash
   cp deploy/.env.example .env
   nano .env
   ```
   Ustaw silne hasło `POSTGRES_PASSWORD` (np. `openssl rand -base64 24`) oraz `CLOUDFLARE_TUNNEL_TOKEN`.
5. Uruchom stos produkcyjny:
   ```bash
   docker compose --profile tunnel up -d --build
   ```
6. Weryfikacja na VM:
   ```bash
   docker compose logs -f api
   curl -s http://127.0.0.1:8080/v1/meta | jq .
   curl -s "http://127.0.0.1:8080/v1/stop-groups?q=lubelska+szpital" | jq .
   curl -s "http://127.0.0.1:8080/v1/stop-groups/lubelska-szpital/departures?limit=10" | jq .
   ```
