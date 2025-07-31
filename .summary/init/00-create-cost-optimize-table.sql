-- Create and use the first database
CREATE DATABASE IF NOT EXISTS `800679018866`;
USE 800679018866;

CREATE TABLE cost_optimization_recommendations (
    id INT NOT NULL AUTO_INCREMENT,
    query_title VARCHAR(100) NULL,
    resource_id VARCHAR(255) NULL,
    payer_account_id VARCHAR(20) NULL,
    usage_account_id VARCHAR(20) NULL,
    payer_account_name VARCHAR(20) NULL,
    usage_account_name VARCHAR(20) NULL,
    product_code VARCHAR(50) NULL,
    year CHAR(4) NULL,
    month CHAR(15) NULL,
    potentials_saving_percentage DECIMAL(12,2) NULL,
    potential_savings_usd DECIMAL(12,2) NULL,
    unblended_cost DECIMAL(12,2) NULL,
    amortized_cost DECIMAL(12,2) NULL,
    query_date DATE NULL,
    achieved_savings_usd DECIMAL(12,2) NULL DEFAULT 0.00,
    current_config JSON NULL,
    recommended_config JSON NULL,
    implementation_details JSON NULL,
    last_updated TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    Source CHAR(4) NULL,
    
    PRIMARY KEY (id),
    INDEX idx_query_title (query_title)
);

-- Create and use the second database
CREATE DATABASE IF NOT EXISTS `392705528955`;
USE 392705528955;

CREATE TABLE cost_optimization_recommendations (
    id INT NOT NULL AUTO_INCREMENT,
    query_title VARCHAR(100) NULL,
    resource_id VARCHAR(255) NULL,
    payer_account_id VARCHAR(20) NULL,
    usage_account_id VARCHAR(20) NULL,
    payer_account_name VARCHAR(20) NULL,
    usage_account_name VARCHAR(20) NULL,
    product_code VARCHAR(50) NULL,
    year CHAR(4) NULL,
    month CHAR(15) NULL,
    potentials_saving_percentage DECIMAL(12,2) NULL,
    potential_savings_usd DECIMAL(12,2) NULL,
    unblended_cost DECIMAL(12,2) NULL,
    amortized_cost DECIMAL(12,2) NULL,
    query_date DATE NULL,
    achieved_savings_usd DECIMAL(12,2) NULL DEFAULT 0.00,
    current_config JSON NULL,
    recommended_config JSON NULL,
    implementation_details JSON NULL,
    last_updated TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    Source CHAR(4) NULL,
    
    PRIMARY KEY (id),
    INDEX idx_query_title (query_title)
);

