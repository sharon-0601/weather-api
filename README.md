# weather-api# Real-time Weather Data Pipeline (AWS + Snowflake)

This project demonstrates a real-time weather data pipeline that collects weather data from the OpenWeather API and processes it for analytics.

## Architecture
OpenWeather API → EventBridge → AWS Lambda → DynamoDB → S3 → Snowflake

## Technologies
- Python
- API Integration
- Data Processing Concepts

## Features
- Sample weather data ingestion
- API request structure demonstration

## Sample Code

```python
import requests

API_KEY = "YOUR_API_KEY"  
city = "Berlin"

url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print("Temperature:", data["main"]["temp"])
    print("Humidity:", data["main"]["humidity"])
else:
    print("Error fetching weather data")
