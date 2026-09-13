# PACE BMS RS232 Monitor

Een Python-monitor voor drie PACE LiFePO4-BMS'en met elk een eigen RS232-USB-kabel. Het programma leest de packs live uit, publiceert de waarden via MQTT met Home Assistant Discovery en biedt een lokale webportal voor monitoring, historie, diagnose en het lezen of schrijven van ondersteunde parameters.



<img width="1623" height="1206" alt="dashboard_webportal" src="https://github.com/user-attachments/assets/a0dbbc29-98db-434c-9926-8b8aa98970b6" />

Deze versie is ontwikkeld en getest met drie PACE-BMS'en die zichzelf identificeren als:

```text
P16S200A-31280-1.30C
```

Andere P16S200A-uitvoeringen gebruiken soms dezelfde protocolfamilie, maar kunnen andere offsets, functies of elektrische eigenschappen hebben. Controleer daarom eerst alle uitlezingen voordat je parameters schrijft.

## Functies

- Live uitlezen van BMS1, BMS2 en BMS3 via drie afzonderlijke RS232-kabels.
- Automatisch opnieuw verbinden wanneer een USB-adapter wordt losgenomen of na een reboot een ander `ttyUSB`-nummer krijgt.
- Controle van het fysieke BMS-adres voordat parameteropdrachten worden uitgevoerd.
- MQTT-statusberichten en Home Assistant MQTT Discovery.
- Webportal met SOC-, stroom- en spanningsgrafieken over de laatste twee uur.
- Persistente SQLite-historie die service-restarts en reboots overleeft.
- JSON-export van alle aanwezige gebeurtenissen, de laatste 1, 6, 12 of 24 uur, vandaag, gisteren of een zelfgekozen periode.
- Celspanningen, hoogste en laagste cel, celdelta, temperaturen, MOSFET-status, waarschuwingen en beveiligingen.
- Werkelijke balanceerbits voor cellen C01 tot en met C16.
- Parameterpanelen per BMS voor onder meer OV, UV, balanceren, slaapinstellingen, temperaturen, full-charge/SOC en de BMS-klok.
- Instellingen worden na schrijven opnieuw gelezen en gecontroleerd.

## Wat de statusweergave betekent

De gebruikte 39-byte `0x44`-statusindeling is:

| Byte | Betekenis |
|---:|---|
| 29 | Protection 1 |
| 30 | Protection 2; bit `0x80` wordt door deze BMS als volstatus gebruikt |
| 31 | Instructions/system; bit `0x20` volgt de laadstatus |
| 32 | Control/configuratie, alleen diagnostisch gebruikt |
| 33 | Fault-status, raw/diagnostisch behandeld |
| 34 | Balanceerbits C01–C08 |
| 35 | Balanceerbits C09–C16 |
| 36 | Warning 1 |
| 37 | Warning 2 |
| 38 | Extra firmwarebyte |

De balanceerbits geven aan welke cellen de BMS als balancerend rapporteert. Ze bewijzen niet of de hardware actief of passief balanceert. De BMS kan deze bits ook bij 0 A en na de volmelding blijven zetten.

<img width="800" height="397" alt="balancing" src="https://github.com/user-attachments/assets/a4128819-50fb-471a-9191-e11b986b7378" />

De portal toont geen berekende CAN Charge Current Limit. De eerder onderzochte instruction/control-bits volgden de numerieke CCL-stappen niet betrouwbaar. De lokale stroombegrenzer en de via CAN gerapporteerde CCL moeten daarom als afzonderlijke functies worden beschouwd.

De volstatus betekent evenmin dat FCC opnieuw wordt berekend. Tijdens tests bereikten alle drie de packs hun voldetectie en 100% SOC, terwijl FCC ongewijzigd bleef. FCC kan door deze versie worden gelezen, maar niet geschreven.

## Benodigdheden

- Linux-computer of Raspberry Pi met Python 3.9 of nieuwer.
- Per aangesloten BMS een geschikte RS232-naar-USB-adapter en correcte PACE-kabel.
- Toegang tot de groep `dialout` voor seriële poorten.
- Optioneel: een MQTT-broker, bijvoorbeeld Mosquitto, en Home Assistant.

RS232 is elektrisch niet hetzelfde als TTL-UART of RS485. Gebruik de kabel en pinout die bij de BMS horen.

## USB-poorten bepalen

Sluit de adapters aan en zoek de stabiele paden:

```bash
ls -l /dev/serial/by-path/
```

Gebruik de volledige paden uit de linkerkolom. Paden onder `/dev/serial/by-path/` zijn betrouwbaarder dan `/dev/ttyUSB0`, omdat `ttyUSB`-nummers na losnemen of rebooten kunnen veranderen.

Noteer welk fysiek BMS-adres aan ieder pad zit. Deze versie verwacht:

| Logisch pack | Verwacht PACE-adres |
|---|---:|
| BMS1 | 1 |
| BMS2 | 2 |
| BMS3 | 3 |

De software controleert dat adres voordat zij de verbinding accepteert. Hierdoor wordt een schrijfopdracht niet stilzwijgend naar het verkeerde pack gestuurd.

## Installatie

Maak een eigen servicegebruiker en installatiemap:

```bash
sudo useradd --system --home /opt/pace-bms --shell /usr/sbin/nologin pace-bms
sudo usermod -aG dialout pace-bms
sudo mkdir -p /opt/pace-bms /var/lib/pace-bms
sudo chown pace-bms:pace-bms /opt/pace-bms /var/lib/pace-bms
```

Kopieer de bestanden uit deze repository naar `/opt/pace-bms` en maak de Python-omgeving:

```bash
cd /opt/pace-bms
sudo -u pace-bms python3 -m venv .venv
sudo -u pace-bms .venv/bin/pip install -r requirements.txt
sudo chmod +x pace_mqtt.py
```

Maak daarna de lokale configuratie:

```bash
sudo cp pace-bms.env.example /etc/pace-bms.env
sudo chmod 600 /etc/pace-bms.env
sudo nano /etc/pace-bms.env
```

Vervang de drie `PACE_SERIAL_BMS...`-waarden door de gevonden `/dev/serial/by-path/`-paden en vul de eigen MQTT-gegevens in.

Installeer en start de service:

```bash
sudo cp pace-bmsrs232.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pace-bmsrs232
sudo systemctl status pace-bmsrs232 --no-pager
```

Bekijk bij problemen de laatste logregels:

```bash
sudo journalctl -u pace-bmsrs232 -n 100 --no-pager
```

Na een aanpassing aan `/etc/pace-bms.env`:

```bash
sudo systemctl restart pace-bmsrs232
```

De webportal is standaard bereikbaar op:

```text
http://IP-ADRES-VAN-DE-SERVER:8080/
```

De portal heeft zelf geen gebruikerslogin. Stel hem niet rechtstreeks vanaf internet beschikbaar. Gebruik voor toegang buiten het lokale netwerk een VPN of een reverse proxy met authenticatie en HTTPS.

## MQTT instellen

MQTT-gegevens horen uitsluitend in `/etc/pace-bms.env`. Plaats het echte configuratiebestand niet in Git.

Voorbeeld:

```ini
PACE_MQTT_HOST=192.168.1.10
PACE_MQTT_PORT=1883
PACE_MQTT_USER=pace-monitor
PACE_MQTT_PASSWORD=gebruik-hier-een-eigen-sterk-wachtwoord
PACE_MQTT_BASE_TOPIC=bmsrs232
```

Betekenis:

| Variabele | Doel |
|---|---|
| `PACE_MQTT_HOST` | Hostnaam of IP-adres van de eigen broker |
| `PACE_MQTT_PORT` | Brokerpoort, meestal `1883` |
| `PACE_MQTT_USER` | MQTT-gebruiker; leeg laten wanneer de broker geen login gebruikt |
| `PACE_MQTT_PASSWORD` | Wachtwoord van die MQTT-gebruiker |
| `PACE_MQTT_BASE_TOPIC` | Basis van alle state- en availabilitytopics |

Wanneer `PACE_MQTT_USER` leeg is, probeert het programma zonder gebruikersnaam en wachtwoord te verbinden.

De belangrijkste topics volgen deze structuur:

```text
bmsrs232/availability
bmsrs232/bms1/availability
bmsrs232/bms1/<meting>
bmsrs232/bms2/<meting>
bmsrs232/bms3/<meting>
```

Home Assistant Discovery wordt retained gepubliceerd onder:

```text
homeassistant/<component>/<device>/<object>/config
```

Na de eerste succesvolle verbinding zouden de drie BMS-apparaten automatisch in Home Assistant moeten verschijnen. Oude discovery-entiteiten verdwijnen niet altijd vanzelf wanneer je later het basistopic wijzigt; verwijder in dat geval de oude retained discoveryberichten op de broker.

## Eén RS232-kabel

Deze release is ontworpen voor drie gelijktijdige, afzonderlijke kabels. Met slechts één aangesloten kabel blijft de service draaien en werkt de portal, maar alleen het pack waarvan pad én fysiek adres kloppen wordt bijgewerkt. De twee andere verbindingen blijven automatisch opnieuw proberen en melden een fout in de portal.

Eén kabel op de master gebruiken om drie parallelle packs uit te lezen is een andere bedrijfsmodus. Deze release gebruikt die modus niet. Parameterbediening via een gedeelde masterverbinding is bovendien niet zonder aanvullende adresverificatie veilig.

Voor een installatie met slechts één pack kan dezelfde code worden gebruikt, maar de huidige portal en MQTT Discovery blijven drie packs aanmaken. Een echte één-packmodus vraagt nog een kleine software-uitbreiding.

## Parameterbediening

Stop andere programma's die dezelfde seriële poort gebruiken. Twee lezers op één kabel kunnen geldige frames door elkaar halen.

De webportal kan ondersteunde instellingen schrijven. Iedere opdracht wordt naar het geselecteerde BMS gestuurd en daarna teruggelezen. Een time-out na het schrijven is geen bewijs dat de BMS de opdracht niet heeft uitgevoerd; controleer de waarde opnieuw voordat je nogmaals schrijft.

Maak vóór wijzigingen een export of noteer alle bestaande waarden. Fabrieksinstellingen zijn niet automatisch geschikt voor iedere accucel, packcapaciteit of omvormer.

## Historie en exports

De meetgrafieken gebruiken een SQLite-database met twee uur samples. Status- en parametergebeurtenissen worden langer bewaard en kunnen vanuit de portal als JSON worden gedownload.

De database staat standaard op:

```text
/var/lib/pace-bms/pace_history.sqlite3
```

Wijzig dit desgewenst met `PACE_HISTORY_DB`. De servicegebruiker moet schrijfrechten hebben op de bovenliggende map.

JSON-exports kunnen ruwe BMS-status, celspanningen, tijden en parameterwijzigingen bevatten. Controleer deze bestanden voordat je ze openbaar deelt.

## Stoppen en verwijderen

Service stoppen of opnieuw starten:

```bash
sudo systemctl stop pace-bmsrs232
sudo systemctl restart pace-bmsrs232
```

Automatisch starten uitschakelen:

```bash
sudo systemctl disable --now pace-bmsrs232
```

## Beveiliging en privacy

- Commit `/etc/pace-bms.env`, echte MQTT-wachtwoorden en statuslogs niet.
- Gebruik voor dit programma bij voorkeur een afzonderlijke MQTT-gebruiker met alleen toegang tot het gekozen basistopic en Home Assistant Discovery.
- Publiceer geen eerder gebruikte brokerwachtwoorden. Wijzig een wachtwoord onmiddellijk wanneer het ooit in een script, chat, screenshot of Git-commit heeft gestaan.
- De `.gitignore` sluit lokale databases, exports, virtuele omgevingen en gangbare configuratiebestanden uit.

## Bekende beperkingen

- Alleen de genoemde PACE-protocolvariant en 16-celstatusindeling zijn onderzocht.
- Actief versus passief balanceren is niet uit de statusbits af te leiden.
- De numerieke CAN CCL wordt nog niet uitgelezen.
- FCC wordt gelezen en gelogd, maar niet geschreven; het precieze FCC-leeralgoritme is onbekend.
- De volstatus en 100% SOC veroorzaken niet noodzakelijk een FCC-herberekening.
- De webportal heeft geen ingebouwde authenticatie.

## Verantwoord gebruik

Een BMS is een beveiligingscomponent. Onjuiste spannings-, stroom- of temperatuurgrenzen kunnen cellen beschadigen of beveiligingen onbruikbaar maken. Vergelijk wijzigingen altijd met de specificaties van de gebruikte cellen en de packbouwer. Deze software vervangt geen elektrische beveiligingen, zekeringen of toezicht tijdens experimentele parameterwijzigingen.
