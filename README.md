# Order Processing Microservice

## Overview
This application is a Kafka consumer written in Go, which consumes order messages from the `raw-orders` topic, processes them by adding a "Processed" status, and then publishes the processed messages to the `processed-orders` topic. It also ensures the necessary Kafka topics and consumer groups are created if they don't exist.

## Prerequisites
To run this application, you will need:
- Go (version 1.21 or higher)
- Kafka server
- Docker (optional, for running Kafka locally)

## Installation

### Step 1: Install Go
Download and install Go from the [official website](https://golang.org/dl/).

### Step 2: Setup Kafka
You can set up Kafka locally using Docker.

### Step 3: Run the application locally
Run
```go
go run main.go
```

### Usage
Once the application is running, it will continuously consume messages from the raw-orders topic, process them, and publish them to the processed-orders topic. Ensure your Kafka producer is configured to send messages to raw-orders.

### Configuration
No additional configuration is needed beyond setting up Kafka and ensuring the topics are created as specified in the application.


