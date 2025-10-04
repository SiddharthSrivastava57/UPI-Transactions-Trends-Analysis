# UPI Transactions Trends Analysis 📊

A comprehensive data analysis project exploring UPI (Unified Payments Interface) transaction patterns, fraud detection, and behavioral insights using Python, SQL, and Tableau.

## 📋 Project Overview

This project analyzes 250,000 UPI transactions from 2024 to uncover:
- **Transaction Patterns**: Peak hours, popular merchant categories, and spending behaviors
- **Fraud Analysis**: Risk assessment by age groups, banks, and transaction types
- **Demographic Insights**: Spending patterns across different age groups and states
- **Device & Network Analysis**: Performance metrics across different platforms
- **Time Series Trends**: Monthly spending patterns and seasonal variations


## 📊 Dataset Description

The dataset contains **250,000 UPI transactions** with the following key attributes:

| Column | Description | Type |
|--------|-------------|------|
| `transaction_id` | Unique transaction identifier | String |
| `timestamp` | Transaction date and time | DateTime |
| `transaction_type` | P2P, P2M, Bill Payment | String |
| `merchant_category` | 10 categories (Food, Shopping, etc.) | String |
| `amount` | Transaction amount in INR | Integer |
| `transaction_status` | SUCCESS, FAILED, PENDING | String |
| `sender_age_group` | 5 age groups (18-25, 26-35, etc.) | String |
| `receiver_age_group` | 5 age groups | String |
| `sender_state` | 8 Indian states | String |
| `sender_bank` | 6 major banks | String |
| `receiver_bank` | 6 major banks | String |
| `device_type` | Android, iOS | String |
| `network_type` | 3G, 4G, 5G, WiFi | String |
| `fraud_flag` | Binary fraud indicator | Integer |
| `hour_of_day` | Transaction hour (0-23) | Integer |
| `day_of_week` | Day of the week | String |
| `is_weekend` | Weekend indicator | Integer |

## 🔍 Key Insights

### 💰 Spending Patterns
- **Education** leads in average transaction value (₹5,094)
- **26-35 age group** shows highest fraud incidents (163 cases)
- **Peak transaction hours**: 15:00-20:00 (Evening)

### 🚨 Fraud Analysis
- **Fraud rate varies by state**: Highest in certain regions
- **Bank pairs** show different risk levels
- **Weekend vs Weekday** patterns differ significantly

### 📱 Technology Trends
- **Android dominance**: Higher transaction volume
- **4G network** most commonly used
- **Device preferences** vary by age group

## 🛠️ Analysis Components

### 1. **Python Analysis** 
- Data exploration and cleaning
- Statistical analysis

### 2. **SQL Analytics** 
- Advanced queries for business insights
- Fraud pattern analysis
- Revenue optimization queries
- Performance metrics

### 3. **Tableau Dashboard**
- Interactive visualizations
- Real-time filtering
- Executive summary views
- Drill-down capabilities

## 📈 Business Applications

- **Risk Management**: Identify high-risk transaction patterns
- **Marketing Strategy**: Target specific demographics and time periods
- **Product Development**: Optimize UPI features based on usage patterns
- **Fraud Prevention**: Implement early warning systems

## 🔧 Technical Stack

- **Data Processing**: Pandas
- **Database**: MySQL
- **Dashboard**: Tableau
- **Development**: Jupyter Notebook, Python

*For detailed analysis and interactive dashboards, please refer to the Jupyter notebook and Tableau workbook.*