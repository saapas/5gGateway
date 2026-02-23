# Project for Distributed Systems course

### Start all services
- docker-compose up --build

### Autoscaler — in a separate terminal
- pip install requests
- python autoscaler.py

### Load test (1000 sensors) — in a separate terminal
- pip install paho-mqtt
- python devices/load_test_1000_sensors.py

Contributors:
Samuel Palovaara
Toni Makkonen
Atte Kiviniemi
Eeli Tavaststjerna
