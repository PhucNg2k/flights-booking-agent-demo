# Flight Booking Agent - Architecture & Technology Overview

## 📋 What is Flight Booking Agent?

Flight Booking Agent is an intelligent AI assistant that helps users search for flights and book tickets through natural conversation. Instead of navigating complex airline websites or filling out forms, users can simply chat with the agent in plain language (English or Vietnamese) to:

- **Search for flights** by date, route, and airline preferences
- **Book flight tickets** with personal information
- **Get answers** about aviation regulations and policies
- **Check flight status** and real-time updates

The agent understands context, asks clarifying questions when needed, and provides accurate, up-to-date information from multiple data sources.

---

## 🎯 Real-World Value & Use Cases

### Problem It Solves
Traditional flight booking requires users to:
- Navigate multiple airline websites
- Fill out complex forms with specific formats
- Manually search through regulations
- Deal with technical jargon and codes

### How the Agent Helps
- **Natural Language Interface**: Users speak naturally - "I want to fly from Da Nang to Hanoi tomorrow morning"
- **Intelligent Understanding**: Agent extracts intent and required information automatically
- **Multi-Source Integration**: Combines real-time flight data, booking systems, and regulation knowledge
- **Conversational Flow**: Asks follow-up questions to clarify missing information
- **Multi-language Support**: Works in both English and Vietnamese

### Real-World Scenarios

**Scenario 1: Flight Search**
- User: "Find flights from Da Nang to Hanoi tomorrow"
- Agent: Understands date, route, searches database, returns available flights with times, gates, and status

**Scenario 2: Flight Booking**
- User: "I want to book flight VN017 for 2 people"
- Agent: Validates flight exists, asks for personal info, processes booking, generates receipt

**Scenario 3: Regulation Questions**
- User: "What's the baggage allowance for international flights?"
- Agent: Searches regulation documents using semantic search, provides accurate policy information



---

## 🏗️ System Architecture Overview

### High-Level Architecture

```
┌─────────────────┐
│   User (Chat)   │
└────────┬────────┘
         │
    ┌────▼─────────────────────────────┐
    │   AI Agent (GPT-4o)              │
    │   - Intent Classification        │
    │   - Tool Selection               │
    │   - Response Generation          │
    └────┬─────────────────────────────┘
         │
    ┌────▼─────────────────────────────┐
    │   Agent Tools                    │
    │   ├─ Query Preparation           │
    │   ├─ Flight Search (MongoDB)     │
    │   ├─ Booking Submission          │
    │   └─ Regulation RAG             │
    └────┬─────────────────────────────┘
         │
    ┌────▼─────────────────────────────┐
    │   Data Sources                   │
    │   ├─ MongoDB (Flight Data)       │
    │   ├─ Elasticsearch (Keywords)   │
    │   └─ Qdrant (Semantic Search)   │
    └──────────────────────────────────┘
```

---

## 🔄 How It Works - Agent Flow

### 1. User Query Processing

**Step 1: Intent Classification**
- Agent analyzes user input to determine intent:
  - **QUERY**: User wants to search for flight information
  - **BOOKING**: User wants to book a ticket
  - **UNKNOWN**: Need clarification

**Step 2: Information Extraction**
- Extracts relevant information from natural language:
  - Dates ("tomorrow", "next week", "2024-12-25")
  - Locations ("Da Nang", "Hanoi", "Ho Chi Minh City")
  - Flight details (flight ID, airline preferences)
  - Personal info (for bookings)

**Step 3: Query Normalization**
- Standardizes extracted data:
  - Converts natural language dates to standard format
  - Matches location names to database values
  - Validates required fields

**Step 4: Tool Selection**
- Agent decides which tool to use based on intent:
  - Flight search → MongoDB_Retriever
  - Booking → SubmitBooking_Tool
  - Regulation questions → RegulationRAG_tool

**Step 5: Response Generation**
- Formats results in user-friendly format
- Provides additional context and suggestions
- Handles errors gracefully

---

## 🛠️ Core Components & Technologies

### 1. Data Collection Pipeline

**Web Crawling**
- **Technology**: Selenium WebDriver + Chrome
- **What it does**: Automatically collects flight schedules from airline websites
- **How it works**: 
  - Crawls flight data for next 7 days
  - Extracts flight times, routes, gates, status
  - Handles dynamic web content and form submissions

**Message Queue (Kafka)**
- **Technology**: Apache Kafka
- **What it does**: Receives and buffers flight data from crawlers
- **Why it's needed**: Decouples data collection from processing, handles high volume

**Stream Processing (Spark)**
- **Technology**: Apache Spark Streaming
- **What it does**: Processes and enriches flight data in real-time
- **How it works**:
  - Reads from Kafka every 5 seconds
  - Validates and cleans data
  - Enriches with airport names, airline names, region mappings
  - Detects flight delays
  - Writes to MongoDB

**Data Enrichment**
- Maps IATA codes (e.g., "DAD", "HAN") to full names
- Adds region information for better search
- Standardizes airline names

---

### 2. Storage Systems

**MongoDB**
- **Purpose**: Primary database for flight information and bookings
- **What it stores**:
  - Real-time flight schedules and status
  - User bookings and reservations
- **Why MongoDB**: Flexible schema, fast queries, good for semi-structured data

**Elasticsearch**
- **Purpose**: Keyword search for regulation documents
- **What it does**: Fast text search using BM25 algorithm
- **Use case**: Finding regulations by specific keywords

**Qdrant**
- **Purpose**: Semantic search for regulation documents
- **What it does**: Vector similarity search using embeddings "text-embedding-3-small"
- **How it works**: Converts text to vectors, finds semantically similar content
- **Use case**: Understanding user questions even with different wording

---

### 3. RAG System (Retrieval Augmented Generation)

**What is RAG?**
RAG combines retrieval (finding relevant information) with generation (creating responses) to answer questions accurately.

**How It Works:**

1. **Data Ingestion**
   - Reads aviation regulation documents
   - Splits text into semantic chunks
   - Generates embeddings (vector representations)

2. **Hybrid Search Strategy**
   - **Semantic Search** (Qdrant): Finds documents with similar meaning
   - **Keyword Search** (Elasticsearch): Finds documents with matching words
   - **Combines both**: Weighted combination (70% semantic, 30% keyword)
   - Returns top 3 most relevant results

3. **Response Generation**
   - Agent uses retrieved regulations to answer user questions
   - Provides accurate, context-aware answers

**Why Hybrid Search?**
- Semantic search handles variations in wording
- Keyword search ensures exact matches
- Together, they provide more accurate results

---

### 4. Agent Tools

The agent has access to 4 specialized tools:

**1. Query_Prep Tool**
- **What it does**: Analyzes user input and extracts structured information
- **How it works**: Uses LLM to convert natural language to structured query format
- **Output**: Intent classification + structured query

**2. MongoDB_Retriever Tool**
- **What it does**: Searches flight database for matching flights
- **How it works**: 
  - Connects to MongoDB
  - Builds query based on user criteria (date, route, airline)
  - Returns matching flights with details
- **Returns**: List of available flights with times, gates, status

**3. SubmitBooking_Tool**
- **What it does**: Processes flight bookings
- **How it works**:
  - Validates flight exists
  - Stores booking information
  - Generates booking receipt
- **Returns**: Confirmation with booking details

**4. RegulationRAG_tool**
- **What it does**: Answers questions about aviation regulations
- **How it works**: Uses hybrid search to find relevant regulations
- **Returns**: Relevant regulation excerpts to answer user questions

---

## 🔄 Complete Data Flow

### Flight Data Pipeline
```
Airline Website
    ↓ (Selenium Crawler)
Kafka Topic
    ↓ (Real-time streaming)
Spark Processing
    ├─ Data Validation
    ├─ Code Mapping
    ├─ Delay Detection
    └─ Data Enrichment
    ↓
MongoDB Database
    ↓
Available for Agent Queries
```

### User Query Flow
```
User Types Question
    ↓
GPT-4o Agent Receives Input
    ↓
Intent Classification (GPT-3.5)
    ├─ QUERY → Search flights
    ├─ BOOKING → Book ticket
    └─ REGULATION → Answer question
    ↓
Information Extraction
    ├─ Parse dates, locations
    ├─ Extract flight details
    └─ Normalize data
    ↓
Tool Selection & Execution
    ├─ MongoDB_Retriever (search)
    ├─ SubmitBooking_Tool (book)
    └─ RegulationRAG_tool (answer)
    ↓
Response Generation
    ↓
User Receives Answer
```

### Regulation RAG Flow
```
User Asks Regulation Question
    ↓
Agent Calls RegulationRAG_tool
    ↓
Hybrid Search
    ├─ Semantic Search (Qdrant)
    └─ Keyword Search (Elasticsearch)
    ↓
Score & Rank Results
    ↓
Retrieve Top 3 Regulations
    ↓
Agent Generates Response
    ↓
User Gets Answer
```

---

## 🛠️ Technology Stack

### AI/ML Layer
- **LlamaIndex**: Agent framework, workflow management, tool integration
- **OpenAI GPT-4o**: Main agent for reasoning and tool calling
- **OpenAI GPT-3.5-turbo**: Intent classification and query parsing
- **OpenAI Embeddings**: Semantic search (text-embedding-3-small)

### Data Processing
- **Apache Spark**: Real-time stream processing
- **Apache Kafka**: Message queue for data pipeline
- **Zookeeper**: Kafka coordination

### Databases
- **MongoDB**: Flight data and bookings
- **Elasticsearch**: Keyword search
- **Qdrant**: Vector database for semantic search

### Web & UI
- **Selenium**: Web crawling automation
- **Gradio**: Chat interface for users
- **Chrome for Testing**: Headless browser

### Infrastructure
- **Docker Compose**: Container orchestration
- **Python**: Runtime environment

---

## 📊 Key Capabilities

### 1. Intelligent Intent Understanding
- Automatically detects what user wants (search, book, or ask questions)
- Handles ambiguous queries by asking clarifying questions

### 2. Natural Language Processing
- Understands dates in various formats ("tomorrow", "next week", "Dec 25")
- Handles location names with variations
- Supports multiple languages (English/Vietnamese)

### 3. Real-Time Data Access
- Accesses up-to-date flight information
- Processes bookings immediately
- Provides current flight status

### 4. Hybrid Search for Regulations
- Combines semantic and keyword search
- Finds relevant information even with different wording
- Provides accurate, source-backed answers

### 5. Error Handling & Validation
- Validates flight existence before booking
- Handles missing information gracefully
- Provides helpful error messages

### 6. Conversational Flow
- Maintains context across conversation
- Asks follow-up questions when needed
- Confirms important information before actions

---

## 🎯 Business Value

### For Users
- **Ease of Use**: Natural conversation instead of forms
- **Time Saving**: Quick access to flight information
- **Accuracy**: Real-time data and validation
- **Accessibility**: Multi-language support

### For Airlines/Operators
- **Reduced Support Load**: Agent handles common queries
- **24/7 Availability**: Always-on service
- **Consistent Experience**: Standardized responses
- **Data Integration**: Unified access to multiple data sources

### Technical Advantages
- **Scalable Architecture**: Handles high volume with Kafka + Spark
- **Flexible Search**: Hybrid approach improves accuracy
- **Maintainable**: Modular design with clear separation of concerns
- **Extensible**: Easy to add new tools and capabilities

---

## 🚀 Deployment Overview

### Infrastructure Services (Docker Compose)
- Kafka + Zookeeper: Message queue
- MongoDB: Primary database
- Elasticsearch: Keyword search
- Qdrant: Vector database

### Application Components
- **Crawler**: Scheduled job to collect flight data
- **Spark Consumer**: Long-running stream processor
- **Agent**: Gradio web application (port 7860)

### Data Flow
1. Crawler runs periodically → Kafka
2. Spark processes continuously → MongoDB
3. Agent serves user queries → Real-time responses

---

## 📈 Performance Characteristics

- **Real-time Processing**: Spark micro-batches every 5 seconds
- **Fast Search**: Hybrid search returns results in milliseconds
- **Scalable**: Kafka handles high throughput
- **Reliable**: Retry mechanisms and error handling
- **Efficient**: Limits results to top matches for performance

---

## 🔮 Future Enhancements

Potential improvements:
- Integration with payment systems
- Email/SMS notifications
- Multi-airline support
- Advanced recommendation engine
- Voice interface support
- Mobile app integration
