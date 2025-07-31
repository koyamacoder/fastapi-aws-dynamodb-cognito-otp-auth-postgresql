-- Create resource_tag_mappings table
CREATE TABLE IF NOT EXISTS resource_tag_mappings (
    resource_id VARCHAR(255) PRIMARY KEY,
    cur_trucost_rt_app VARCHAR(100),
    cur_trucost_rt_bu VARCHAR(100),
    cur_trucost_rt_env VARCHAR(100),
    cur_trucost_rt_owner VARCHAR(100),
    cur_trucost_rt_name VARCHAR(100),
    user_trucost_rt_app VARCHAR(100),
    user_trucost_rt_bu VARCHAR(100),
    user_trucost_rt_env VARCHAR(100),
    user_trucost_rt_owner VARCHAR(100),
    user_trucost_rt_name VARCHAR(100),
    last_cur_sync TIMESTAMP,
    last_user_update TIMESTAMP,
    updated_by VARCHAR(50)
);

-- Insert statements for the data
INSERT INTO resource_tag_mappings (
    resource_id, 
    cur_trucost_rt_app, 
    cur_trucost_rt_bu, 
    cur_trucost_rt_env, 
    cur_trucost_rt_owner, 
    cur_trucost_rt_name,
    user_trucost_rt_app,
    user_trucost_rt_bu,
    user_trucost_rt_env,
    user_trucost_rt_owner,
    user_trucost_rt_name,
    last_cur_sync,
    last_user_update,
    updated_by
) VALUES 
-- ('arn:aws:glue:ap-south-1:800679018866:crawler/cid-CURCrawler', NULL, NULL, NULL, NULL, NULL, 'updated_app', 'updated_bu', NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('arn:aws:glue:ap-south-1:800679018866:crawler/trucost-CURCrawler', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('cf-templates-1p4nghbqrouq9-ap-south-1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('cid-800679018866-data-local', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('i-02c4f3703fd009efe', 'Payment', 'Operations', 'Production', 'Pawan Tiwari', 'TestServer', 'Payment_new', 'Operations', 'Production', 'Pawan Tiwari', 'TestServer', '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('trucost-800679018866-cur', NULL, NULL, NULL, NULL, 'TrucostCURBucket', NULL, NULL, NULL, NULL, 'TrucostCURBucket', '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('trucost-800679018866-data', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
-- ('trucost-800679018866-data-local', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'); 

('trucost-800679018866-data-local-1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'),
('trucost-800679018866-data-local-2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'); 
('trucost-800679018866-data-local-3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'); 
('trucost-800679018866-data-local-4', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'); 
('trucost-800679018866-data-local-5', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-06-18 18:47:44', '2025-06-21 17:15:09', 'system_sync'); 