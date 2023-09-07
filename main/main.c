#include <stdio.h>
#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"

#include "esp_netif.h"
#include "esp_eth.h"
#include "esp_system.h"
#include "esp_mac.h"
#include "esp_wifi.h"
#include "esp_wpa2.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_app_desc.h"
#include "esp_https_ota.h"
#include "esp_crc.h"
#include "esp_ota_ops.h"
#include "nvs_flash.h"
#include "ethernet_init.h"
#include "esp_eth_netif_glue.h"
#include "esp_smartconfig.h"
#include "sdkconfig.h"

#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "esp_tls.h"
#include "mbedtls/md.h"
#include "cJSON.h"
#include "esp_sntp.h"
#include "mqtt_client.h"

#include "esp_ble_mesh_common_api.h"
#include "esp_ble_mesh_provisioning_api.h"
#include "esp_ble_mesh_networking_api.h"
#include "esp_ble_mesh_local_data_operation_api.h"
#include "esp_ble_mesh_config_model_api.h"
#include "esp_ble_mesh_generic_model_api.h"
#include "esp_ble_mesh_lighting_model_api.h"

#include "ble_mesh_example_init.h"

#define CONFIG_PARTITION "config"

#define min(x, y) ((x) < (y) ? (x) : (y))
#define ishex(a) (((a) >= '0' && (a) <= '9') || ((a) >= 'a' && (a) <= 'f'))
#define tohex(a) (((a) >= '0' && (a) <= '9') ? (a) - '0' : (a) - 'a' + 10)

#define DEF_NVS_STR(key) \
    size_t key##_size = 0; \
    char *key = NULL

#define LOAD_NVS_STR(handle, key) \
    DEF_NVS_STR(key); \
    nvs_get_str(handle, #key, NULL, &key##_size); \
    assert(key##_size); \
    assert( key = pvPortMalloc(key##_size + 1) ); \
    ESP_ERROR_CHECK( nvs_get_str(handle, #key, key, &key##_size) ); \
    key[key##_size] = 0; \
    ESP_LOGI(CONFIG_PARTITION, #key ": %s", key)

#define RETRY_LOAD_OPT_NVS_STR(handle, key) \
    if (nvs_get_str(handle, #key, NULL, &key##_size) == ESP_OK) { \
        key##_found = 1; \
        if (key) { \
            vPortFree(key); \
        } \
        assert( key = pvPortMalloc(key##_size + 1) ); \
        ESP_ERROR_CHECK( nvs_get_str(handle, #key, key, &key##_size) ); \
        key[key##_size] = 0; \
        ESP_LOGI(CONFIG_PARTITION, #key ": %s", key); \
    } \
    else { \
        key##_found = 0; \
        key = NULL; \
    }

#define LOAD_OPT_NVS_STR(handle, key) \
    DEF_NVS_STR(key); \
    int key##_found; \
    RETRY_LOAD_OPT_NVS_STR(handle, key)

#define RETRY_LOAD_OPT_NVS_U16(handle, key) \
    if (nvs_get_u16(handle, #key, &key) == ESP_OK) { \
        key##_found = 1; \
        ESP_LOGI(CONFIG_PARTITION, #key ": %" PRIu16, key); \
    } \
    else { \
        key##_found = 0; \
        key = 0; \
    }

#define LOAD_OPT_NVS_U16(handle, key) \
    uint16_t key; \
    int key##_found; \
    RETRY_LOAD_OPT_NVS_U16(handle, key)

#define CID_ESP 0x02E5

struct esp_eth_netif_glue_t {
    esp_netif_driver_base_t base;
    esp_eth_handle_t eth_driver;
    esp_event_handler_instance_t start_ctx_handler;
    esp_event_handler_instance_t stop_ctx_handler;
    esp_event_handler_instance_t connect_ctx_handler;
    esp_event_handler_instance_t disconnect_ctx_handler;
    esp_event_handler_instance_t get_ip_ctx_handler;
};

struct mqtt_event_handler_args {
    char *gateway_ota_topic;
    char *gateway_ntp_response_topic;
    char *gateway_ota_download_reply_topic;
    char *gateway_job_update_reply_topic;
    char *gateway_job_get_reply_topic;
    char *gateway_job_notify_topic;
    char *gateway_bootstrap_notify_topic;
    char *gateway_config_push_topic;
    char *gateway_config_get_reply_topic;
    char *gateway_set_onoff_topic;
    char *gateway_set_hsl_color_topic;
    char *gateway_property_post_topic;
    char *gateway_property_set_topic;
    char *gateway_property_post_reply_topic;
    char *server_cer;
    char *product_key;
    char *device_name;
    esp_mqtt_client_handle_t client;
    esp_ota_handle_t ota_handle;
    uint32_t stream_id;
    uint32_t file_id;
    int last_progress;
    const esp_partition_t *ota_partition;
    SemaphoreHandle_t gateway_ota_upgrading_sem;
};

struct ota_http_event_handler_args {
    int last_progress;
    size_t recv_bytes;
    size_t total_bytes;
    const char *product_key;
    const char *device_name;
    esp_mqtt_client_handle_t client;
};

static const char *TCPIP_TAG = "tcp/ip";
static const char *SC_TAG = "smartconfig";
static const char *ALIYUN_TAG = "aliyun";
static const char *BLEMESH_TAG = "ble_mesh";

/* FreeRTOS event group to signal when we are connected & ready to make a request */
static EventGroupHandle_t s_wifi_event_group;

/* The event group allows multiple bits for each event,
   but we only care about one event - are we connected
   to the AP with an IP? */
static const int CONNECTED_BIT = BIT0;
static const int ESPTOUCH_DONE_BIT = BIT1;

static uint8_t dev_uuid[16] = { 0xdd, 0xdd };

static esp_ble_mesh_client_t onoff_client;
static esp_ble_mesh_client_t hsl_client;

static esp_ble_mesh_cfg_srv_t config_server = {
    .relay = ESP_BLE_MESH_RELAY_DISABLED,
    .beacon = ESP_BLE_MESH_BEACON_ENABLED,
#if defined(CONFIG_BLE_MESH_FRIEND)
    .friend_state = ESP_BLE_MESH_FRIEND_ENABLED,
#else
    .friend_state = ESP_BLE_MESH_FRIEND_NOT_SUPPORTED,
#endif
#if defined(CONFIG_BLE_MESH_GATT_PROXY_SERVER)
    .gatt_proxy = ESP_BLE_MESH_GATT_PROXY_ENABLED,
#else
    .gatt_proxy = ESP_BLE_MESH_GATT_PROXY_NOT_SUPPORTED,
#endif
    .default_ttl = 7,
    /* 3 transmissions with 20ms interval */
    .net_transmit = ESP_BLE_MESH_TRANSMIT(2, 20),
    .relay_retransmit = ESP_BLE_MESH_TRANSMIT(2, 20),
};

ESP_BLE_MESH_MODEL_PUB_DEFINE(onoff_cli_pub, 2 + 1, ROLE_NODE);
ESP_BLE_MESH_MODEL_PUB_DEFINE(hsl_cli_pub, 2 + 9, ROLE_NODE);

static esp_ble_mesh_model_t root_models[] = {
    ESP_BLE_MESH_MODEL_CFG_SRV(&config_server),
    ESP_BLE_MESH_MODEL_GEN_ONOFF_CLI(&onoff_cli_pub, &onoff_client),
    ESP_BLE_MESH_MODEL_LIGHT_HSL_CLI(&hsl_cli_pub, &hsl_client),
};

static esp_ble_mesh_elem_t elements[] = {
    ESP_BLE_MESH_ELEMENT(0, root_models, ESP_BLE_MESH_MODEL_NONE),
};

static esp_ble_mesh_comp_t composition = {
    .cid = CID_ESP,
    .elements = elements,
    .element_count = ARRAY_SIZE(elements),
};

/* Disable OOB security for SILabs Android app */
static esp_ble_mesh_prov_t provision = {
    .uuid = dev_uuid,
#if 0
    .output_size = 4,
    .output_actions = ESP_BLE_MESH_DISPLAY_NUMBER,
    .input_actions = ESP_BLE_MESH_PUSH,
    .input_size = 4,
#else
    .output_size = 0,
    .output_actions = 0,
#endif
};

static void smartconfig_example_task(void * parm)
{
    do {
        EventBits_t uxBits = xEventGroupWaitBits(s_wifi_event_group, CONNECTED_BIT, true, false, portMAX_DELAY);
        if(uxBits & CONNECTED_BIT) {
            ESP_LOGI(SC_TAG, "Server connected. Stop smartconfig");
            esp_smartconfig_stop();
        }
        else {
            ESP_LOGI(SC_TAG, "Server disconnected. Start smartconfig");
            ESP_ERROR_CHECK( esp_smartconfig_set_type(SC_TYPE_ESPTOUCH) );
            smartconfig_start_config_t cfg = SMARTCONFIG_START_CONFIG_DEFAULT();
            ESP_ERROR_CHECK( esp_smartconfig_start(&cfg) );
            esp_wifi_connect();
        }
    } while (1);
}

static void event_handler(void* arg, esp_event_base_t event_base,
                                int32_t event_id, void* event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        xTaskCreate(smartconfig_example_task, "smartconfig_example_task", 4096, NULL, 3, NULL);
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_CONNECTED) {
        esp_netif_create_ip6_linklocal(arg);
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        esp_wifi_connect();
        xEventGroupClearBits(s_wifi_event_group, CONNECTED_BIT);
    } else if (event_base == SC_EVENT && event_id == SC_EVENT_SCAN_DONE) {
        ESP_LOGI(SC_TAG, "Scan done");
    } else if (event_base == SC_EVENT && event_id == SC_EVENT_FOUND_CHANNEL) {
        ESP_LOGI(SC_TAG, "Found channel");
    } else if (event_base == SC_EVENT && event_id == SC_EVENT_GOT_SSID_PSWD) {
        ESP_LOGI(SC_TAG, "Got SSID and password");

        smartconfig_event_got_ssid_pswd_t *evt = (smartconfig_event_got_ssid_pswd_t *)event_data;
        wifi_config_t wifi_config;
        uint8_t ssid[33] = { 0 };
        uint8_t password[65] = { 0 };
        uint8_t rvd_data[33] = { 0 };

        bzero(&wifi_config, sizeof(wifi_config_t));
        memcpy(wifi_config.sta.ssid, evt->ssid, sizeof(wifi_config.sta.ssid));
        memcpy(wifi_config.sta.password, evt->password, sizeof(wifi_config.sta.password));
        wifi_config.sta.bssid_set = evt->bssid_set;
        if (wifi_config.sta.bssid_set == true) {
            memcpy(wifi_config.sta.bssid, evt->bssid, sizeof(wifi_config.sta.bssid));
        }

        memcpy(ssid, evt->ssid, sizeof(evt->ssid));
        memcpy(password, evt->password, sizeof(evt->password));
        ESP_LOGI(SC_TAG, "SSID:%s", ssid);
        ESP_LOGI(SC_TAG, "PASSWORD:%s", password);
        if (evt->type == SC_TYPE_ESPTOUCH_V2) {
            ESP_ERROR_CHECK( esp_smartconfig_get_rvd_data(rvd_data, sizeof(rvd_data)) );
            ESP_LOGI(SC_TAG, "RVD_DATA:");
            for (int i=0; i<33; i++) {
                printf("%02x ", rvd_data[i]);
            }
            printf("\n");
        }

        ESP_ERROR_CHECK( esp_wifi_disconnect() );
        ESP_ERROR_CHECK( esp_wifi_set_config(WIFI_IF_STA, &wifi_config) );
        esp_wifi_connect();
    } else if (event_base == SC_EVENT && event_id == SC_EVENT_SEND_ACK_DONE) {
        xEventGroupSetBits(s_wifi_event_group, ESPTOUCH_DONE_BIT);
    }
}

/** Event handler for Ethernet events */
static void eth_event_handler(void *arg, esp_event_base_t event_base,
                              int32_t event_id, void *event_data)
{
    /* we can get the ethernet driver handle from event data */
    esp_eth_handle_t eth_handle = *(esp_eth_handle_t *)event_data;
    esp_eth_netif_glue_handle_t glue = arg;
    esp_netif_t *netif = glue->base.netif;

    if (glue->eth_driver == eth_handle) {
        switch (event_id) {
            case ETHERNET_EVENT_CONNECTED: {
                uint8_t mac_addr[6] = {0};
                esp_eth_ioctl(eth_handle, ETH_CMD_G_MAC_ADDR, mac_addr);
                ESP_LOGI(TCPIP_TAG, "Ethernet Link Up");
                ESP_LOGI(TCPIP_TAG, "Ethernet HW Addr %02x:%02x:%02x:%02x:%02x:%02x",
                         mac_addr[0], mac_addr[1], mac_addr[2], mac_addr[3], mac_addr[4], mac_addr[5]);
                esp_netif_create_ip6_linklocal(netif);
                break;
            }

            case ETHERNET_EVENT_DISCONNECTED: {
                ESP_LOGI(TCPIP_TAG, "Ethernet Link Down");
                break;
            }

            case ETHERNET_EVENT_START: {
                ESP_LOGI(TCPIP_TAG, "Ethernet Started");
                break;
            }

            case ETHERNET_EVENT_STOP: {
                ESP_LOGI(TCPIP_TAG, "Ethernet Stopped");
                break;
            }

            default: {
                break;
            }
        }
    }
}

/** Event handler for IP event */
static void ip_event_handler(void *arg, esp_event_base_t event_base,
                                 int32_t event_id, void *event_data)
{
    switch (event_id) {
        case IP_EVENT_GOT_IP6: {
            ip_event_got_ip6_t *event = event_data;

            /* types of ipv6 addresses to be displayed on ipv6 events */
            const char *s_ipv6_addr_types[] = {
                "UNKNOWN",
                "GLOBAL",
                "LINK_LOCAL",
                "SITE_LOCAL",
                "UNIQUE_LOCAL",
                "IPV4_MAPPED_IPV6"
            };

            esp_ip6_addr_type_t ipv6_type = esp_netif_ip6_get_addr_type(&event->ip6_info.ip);
            ESP_LOGI(TCPIP_TAG, "ipv6: [" IPV6STR "] type: %s", IPV62STR(event->ip6_info.ip), s_ipv6_addr_types[ipv6_type]);
            break;
        }
    }
}

static void prov_complete(uint16_t net_idx, uint16_t addr, uint8_t flags, uint32_t iv_index)
{
    ESP_LOGI(BLEMESH_TAG, "net_idx: 0x%04x, addr: 0x%04x", net_idx, addr);
    ESP_LOGI(BLEMESH_TAG, "flags: 0x%02x, iv_index: 0x%08" PRIx32, flags, iv_index);
}

static void example_ble_mesh_provisioning_cb(esp_ble_mesh_prov_cb_event_t event,
                                             esp_ble_mesh_prov_cb_param_t *param)
{
    switch (event) {
    case ESP_BLE_MESH_PROV_REGISTER_COMP_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_PROV_REGISTER_COMP_EVT, err_code %d", param->prov_register_comp.err_code);
        break;
    case ESP_BLE_MESH_NODE_PROV_ENABLE_COMP_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_NODE_PROV_ENABLE_COMP_EVT, err_code %d", param->node_prov_enable_comp.err_code);
        break;
    case ESP_BLE_MESH_NODE_PROV_LINK_OPEN_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_NODE_PROV_LINK_OPEN_EVT, bearer %s",
            param->node_prov_link_open.bearer == ESP_BLE_MESH_PROV_ADV ? "PB-ADV" : "PB-GATT");
        break;
    case ESP_BLE_MESH_NODE_PROV_LINK_CLOSE_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_NODE_PROV_LINK_CLOSE_EVT, bearer %s",
            param->node_prov_link_close.bearer == ESP_BLE_MESH_PROV_ADV ? "PB-ADV" : "PB-GATT");
        break;
    case ESP_BLE_MESH_NODE_PROV_COMPLETE_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_NODE_PROV_COMPLETE_EVT");
        prov_complete(param->node_prov_complete.net_idx, param->node_prov_complete.addr,
            param->node_prov_complete.flags, param->node_prov_complete.iv_index);
        break;
    case ESP_BLE_MESH_NODE_PROV_RESET_EVT:
        break;
    case ESP_BLE_MESH_NODE_SET_UNPROV_DEV_NAME_COMP_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_NODE_SET_UNPROV_DEV_NAME_COMP_EVT, err_code %d", param->node_set_unprov_dev_name_comp.err_code);
        break;
    default:
        break;
    }
}

static void example_ble_mesh_generic_client_cb(esp_ble_mesh_generic_client_cb_event_t event,
                                               esp_ble_mesh_generic_client_cb_param_t *param)
{
    ESP_LOGI(BLEMESH_TAG, "Generic client, event %u, error code %d, opcode is 0x%04" PRIx32,
        event, param->error_code, param->params->opcode);

    switch (event) {
    case ESP_BLE_MESH_GENERIC_CLIENT_GET_STATE_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_GENERIC_CLIENT_GET_STATE_EVT");
        if (param->params->opcode == ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_GET) {
            ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_GET, onoff %d", param->status_cb.onoff_status.present_onoff);
        }
        break;
    case ESP_BLE_MESH_GENERIC_CLIENT_SET_STATE_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_GENERIC_CLIENT_SET_STATE_EVT");
        if (param->params->opcode == ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_GET) {
            ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_GET, onoff %d", param->status_cb.onoff_status.present_onoff);
        }
        break;
    case ESP_BLE_MESH_GENERIC_CLIENT_PUBLISH_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_GENERIC_CLIENT_PUBLISH_EVT");
        if (param->params->opcode == ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_GET) {
            ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_GET, onoff %d", param->status_cb.onoff_status.present_onoff);
        }
        break;
    case ESP_BLE_MESH_GENERIC_CLIENT_TIMEOUT_EVT:
        ESP_LOGE(BLEMESH_TAG, "ESP_BLE_MESH_GENERIC_CLIENT_TIMEOUT_EVT");
        if (param->params->opcode == ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_SET) {
            /* If failed to get the response of Generic OnOff Set, resend Generic OnOff Set  */
            // example_ble_mesh_send_gen_onoff_set();
        }
        break;
    default:
        break;
    }
}

static void example_ble_mesh_config_server_cb(esp_ble_mesh_cfg_server_cb_event_t event,
                                              esp_ble_mesh_cfg_server_cb_param_t *param)
{
    if (event == ESP_BLE_MESH_CFG_SERVER_STATE_CHANGE_EVT) {
        switch (param->ctx.recv_op) {
        case ESP_BLE_MESH_MODEL_OP_APP_KEY_ADD:
            ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_MODEL_OP_APP_KEY_ADD");
            ESP_LOGI(BLEMESH_TAG, "net_idx 0x%04x, app_idx 0x%04x",
                param->value.state_change.appkey_add.net_idx,
                param->value.state_change.appkey_add.app_idx);
            ESP_LOG_BUFFER_HEX("AppKey", param->value.state_change.appkey_add.app_key, 16);
            ESP_ERROR_CHECK(esp_ble_mesh_node_bind_app_key_to_local_model(esp_ble_mesh_get_primary_element_address(), BLE_MESH_CID_NVAL, ESP_BLE_MESH_MODEL_ID_GEN_ONOFF_CLI, param->value.state_change.appkey_add.app_idx));
            ESP_ERROR_CHECK(esp_ble_mesh_node_bind_app_key_to_local_model(esp_ble_mesh_get_primary_element_address(), BLE_MESH_CID_NVAL, ESP_BLE_MESH_MODEL_ID_LIGHT_HSL_CLI, param->value.state_change.appkey_add.app_idx));
            break;
        case ESP_BLE_MESH_MODEL_OP_MODEL_APP_BIND:
            ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_MODEL_OP_MODEL_APP_BIND");
            ESP_LOGI(BLEMESH_TAG, "elem_addr 0x%04x, app_idx 0x%04x, cid 0x%04x, mod_id 0x%04x",
                param->value.state_change.mod_app_bind.element_addr,
                param->value.state_change.mod_app_bind.app_idx,
                param->value.state_change.mod_app_bind.company_id,
                param->value.state_change.mod_app_bind.model_id);
            if (param->value.state_change.mod_app_bind.company_id == 0xFFFF &&
                param->value.state_change.mod_app_bind.model_id == ESP_BLE_MESH_MODEL_ID_GEN_ONOFF_CLI) {
            }
            break;
        default:
            break;
        }
    }
}

static void example_ble_mesh_light_client_cb(esp_ble_mesh_light_client_cb_event_t event,
                                             esp_ble_mesh_light_client_cb_param_t *param)
{
    ESP_LOGI(BLEMESH_TAG, "Light client, event %u, error code %d, opcode is 0x%04" PRIx32,
        event, param->error_code, param->params->opcode);

    switch (event) {
    case ESP_BLE_MESH_LIGHT_CLIENT_GET_STATE_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_LIGHT_CLIENT_GET_STATE_EVT");
        break;
    case ESP_BLE_MESH_LIGHT_CLIENT_SET_STATE_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_LIGHT_CLIENT_SET_STATE_EVT");
        break;
    case ESP_BLE_MESH_LIGHT_CLIENT_PUBLISH_EVT:
        ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_LIGHT_CLIENT_PUBLISH_EVT");
        if (param->params->opcode == ESP_BLE_MESH_MODEL_OP_LIGHT_HSL_STATUS) {
            ESP_LOGI(BLEMESH_TAG, "ESP_BLE_MESH_MODEL_OP_LIGHT_HSL_STATUS, hue %0x%04" PRIx16 " saturation %0x%04" PRIx16 " lightness %0x%04" PRIx16, param->status_cb.hsl_status.hsl_hue, param->status_cb.hsl_status.hsl_saturation, param->status_cb.hsl_status.hsl_lightness);
        }
        break;
    case ESP_BLE_MESH_LIGHT_CLIENT_TIMEOUT_EVT:
        ESP_LOGE(BLEMESH_TAG, "ESP_BLE_MESH_LIGHT_CLIENT_TIMEOUT_EVT");
        if (param->params->opcode == ESP_BLE_MESH_MODEL_OP_LIGHT_HSL_SET) {
            /* If failed to get the response of Light HSL Set, resend Light HSL Set  */
            // example_ble_mesh_send_light_hsl_set();
        }
        break;
    default:
        break;
    }
}

static esp_err_t ble_mesh_init(void)
{
    esp_err_t err = ESP_OK;

    esp_ble_mesh_register_prov_callback(example_ble_mesh_provisioning_cb);
    esp_ble_mesh_register_generic_client_callback(example_ble_mesh_generic_client_cb);
    esp_ble_mesh_register_config_server_callback(example_ble_mesh_config_server_cb);
    esp_ble_mesh_register_light_client_callback(example_ble_mesh_light_client_cb);

    err = esp_ble_mesh_init(&provision, &composition);
    if (err != ESP_OK) {
        ESP_LOGE(BLEMESH_TAG, "Failed to initialize mesh stack (err %d)", err);
        return err;
    }

    if (!esp_ble_mesh_node_is_provisioned() || !esp_ble_mesh_node_get_local_app_key(0)) {
        err = esp_ble_mesh_node_prov_enable(ESP_BLE_MESH_PROV_ADV | ESP_BLE_MESH_PROV_GATT);
        if (err != ESP_OK) {
            ESP_LOGE(BLEMESH_TAG, "Failed to enable mesh node (err %d)", err);
            return err;
        }
    }
    else {
        esp_ble_mesh_node_prov_disable(ESP_BLE_MESH_PROV_ADV | ESP_BLE_MESH_PROV_GATT);
    }

    ESP_LOGI(BLEMESH_TAG, "BLE Mesh Node initialized");

    return err;
}

static void parse_guider_response(const char *data, int len, nvs_handle_t nvs_handle)
{
    cJSON *root_node = cJSON_ParseWithLength(data, (size_t)len);
    if (cJSON_IsObject(root_node)) {
        cJSON *data_node = cJSON_GetObjectItem(root_node, "data");
        if (cJSON_IsObject(data_node)) {
            cJSON *resources_node = cJSON_GetObjectItem(data_node, "resources");
            if (cJSON_IsObject(resources_node)) {
                cJSON *mqtt_node = cJSON_GetObjectItem(resources_node, "mqtt");
                if (cJSON_IsObject(mqtt_node)) {
                    cJSON *host_node = cJSON_GetObjectItem(mqtt_node, "host");
                    cJSON *port_node = cJSON_GetObjectItem(mqtt_node, "port");
                    if (cJSON_IsString(host_node) && cJSON_IsNumber(port_node)) {
                        const char * host = cJSON_GetStringValue(host_node);
                        uint16_t port = (uint16_t)cJSON_GetNumberValue(port_node);
                        if (host && port) {
                            ESP_ERROR_CHECK( nvs_set_str(nvs_handle, "host", host) );
                            ESP_ERROR_CHECK( nvs_set_u16(nvs_handle, "port", port) );
                            ESP_ERROR_CHECK( nvs_commit(nvs_handle) );
                        }
                    }
                }
            }
        }
    }
    cJSON_Delete(root_node);
}

static esp_err_t http_event_handler(esp_http_client_event_t *event)
{
    static char *output_buffer;  // Buffer to store response of http request from event handler
    static int output_len;       // Stores number of bytes read
    switch(event->event_id) {
        case HTTP_EVENT_ERROR:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_ERROR");
            break;
        case HTTP_EVENT_ON_CONNECTED:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_ON_CONNECTED");
            break;
        case HTTP_EVENT_HEADER_SENT:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_HEADER_SENT");
            break;
        case HTTP_EVENT_ON_HEADER:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_ON_HEADER, key=%s, value=%s", event->header_key, event->header_value);
            break;
        case HTTP_EVENT_ON_DATA:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_ON_DATA, len=%d", event->data_len);
            /*
             *  Check for chunked encoding is added as the URL for chunked encoding used in this example returns binary data.
             *  However, event handler can also be used in case chunked encoding is used.
             */
            if (!esp_http_client_is_chunked_response(event->client)) {
                if (output_buffer == NULL) {
                    output_buffer = (char *) malloc(esp_http_client_get_content_length(event->client));
                    output_len = 0;
                    if (output_buffer == NULL) {
                        ESP_LOGE(ALIYUN_TAG, "Failed to allocate memory for output buffer");
                        return ESP_FAIL;
                    }
                }
                memcpy(output_buffer + output_len, event->data, event->data_len);
                output_len += event->data_len;
            }

            break;
        case HTTP_EVENT_ON_FINISH:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_ON_FINISH");
            if (output_buffer != NULL) {
                // Response is accumulated in output_buffer. Uncomment the below line to print the accumulated response
                ESP_LOGI(ALIYUN_TAG, "%.*s", output_len, output_buffer);
                
                parse_guider_response(output_buffer, output_len, (nvs_handle_t)event->user_data);

                free(output_buffer);
                output_buffer = NULL;
            }
            output_len = 0;
            break;
        case HTTP_EVENT_DISCONNECTED:
            ESP_LOGI(ALIYUN_TAG, "HTTP_EVENT_DISCONNECTED");
            int mbedtls_err = 0;
            esp_err_t err = esp_tls_get_and_clear_last_error((esp_tls_error_handle_t)event->data, &mbedtls_err, NULL);
            if (err != 0) {
                ESP_LOGI(ALIYUN_TAG, "Last esp error code: 0x%x", err);
                ESP_LOGI(ALIYUN_TAG, "Last mbedtls failure: 0x%x", mbedtls_err);
            }
            if (output_buffer != NULL) {
                free(output_buffer);
                output_buffer = NULL;
            }
            output_len = 0;
            break;
        case HTTP_EVENT_REDIRECT:
            ESP_LOGD(ALIYUN_TAG, "HTTP_EVENT_REDIRECT");
            break;
    }

    return ESP_OK;
}

static void time_update_routine(void *arg)
{
    struct mqtt_event_handler_args *args = arg;

    while (esp_mqtt_client_subscribe(args->client, args->gateway_ntp_response_topic, 0) < 0) {
        vTaskDelay(pdMS_TO_TICKS(2000));
    }

    for (;;) {
        char *data = NULL;
        int data_len = asprintf(&data, "{\"deviceSendTime\":%" PRIu64 "}", time(NULL));
        assert(data);

        char *topic = NULL;
        asprintf(&topic, "/ext/ntp/%s/%s/request", args->product_key, args->device_name);
        assert(topic);
        while (esp_mqtt_client_publish(args->client, topic, data, data_len, 0, 0) < 0) {
            ESP_LOGE(ALIYUN_TAG, "time update failed");
            vTaskDelay(pdMS_TO_TICKS(2000));
        }

        free(topic);
    
        ESP_LOGI(ALIYUN_TAG, "request time update: %s", data);
        free(data);

        vTaskDelay(pdMS_TO_TICKS(60000 * 30));
    }
}

static void do_time_update(const char *data, int len)
{
    uint64_t device_recv_time = time(NULL);

    cJSON *root_node = cJSON_ParseWithLength(data, len);
    if (cJSON_IsObject(root_node)) {
        cJSON *device_send_time_node = cJSON_GetObjectItem(root_node, "deviceSendTime");
        cJSON *server_recv_time_node = cJSON_GetObjectItem(root_node, "serverRecvTime");
        cJSON *server_send_time_node = cJSON_GetObjectItem(root_node, "serverSendTime");

        if (cJSON_IsNumber(device_send_time_node) && cJSON_IsNumber(server_recv_time_node) && cJSON_IsNumber(server_send_time_node)) {
            uint64_t device_send_time = cJSON_GetNumberValue(device_send_time_node);
            uint64_t server_recv_time = cJSON_GetNumberValue(server_recv_time_node);
            uint64_t server_send_time = cJSON_GetNumberValue(server_send_time_node);

            uint64_t now = (server_recv_time + server_send_time + device_recv_time - device_send_time) / 2;

            struct timeval tv = {
                .tv_sec = now / 1000,
                .tv_usec = (now % 1000) * 1000,
            };

            setenv("TZ", "UTC-8", 1);
            settimeofday(&tv, NULL);

            time_t t = time(NULL);
            struct tm tm;
            localtime_r(&t, &tm);

            char datatime[30];
            size_t datatime_len = strftime(datatime, sizeof datatime, "%F %T UTC%z", &tm);
            ESP_LOGI(ALIYUN_TAG, "%.*s", datatime_len, datatime);
        }
    }
    cJSON_Delete(root_node);
}

static uint32_t get_next_alink_id() {
    static uint32_t alink_id;
    return alink_id++;
}

static void publish_alink_msg(esp_mqtt_client_handle_t client, const char *topic, cJSON *sys_node, cJSON *params_node, const char *method, int qos, int retain) {
    cJSON *root_node = cJSON_CreateObject();
    cJSON_AddNumberToObject(root_node, "id", get_next_alink_id());
    cJSON_AddStringToObject(root_node, "version", "1.0");
    if (sys_node) {
        cJSON_AddItemToObject(root_node, "sys", sys_node);
    }

    if (params_node) {
        cJSON_AddItemToObject(root_node, "params", params_node);
    }

    if (method) {
        cJSON_AddStringToObject(root_node, "method", method);
    }

    char *msg = cJSON_PrintUnformatted(root_node);
    cJSON_Delete(root_node);
    while (esp_mqtt_client_publish(client, topic, msg, strlen(msg), qos, retain) < 0);
    cJSON_free(msg);
}

static void report_progress(esp_mqtt_client_handle_t client, const char *product_key, const char *device_name, int progress, int qos, int retain) {
    cJSON *params_node = cJSON_CreateObject();
    cJSON_AddStringToObject(params_node, "desc", "");
    cJSON_AddNumberToObject(params_node, "step", progress);
    char *topic = NULL;
    asprintf(&topic, "/ota/device/progress/%s/%s", product_key, device_name);
    assert(topic);
    publish_alink_msg(client, topic, NULL, params_node, NULL, qos, retain);
    free(topic);
}

static void report_version(esp_mqtt_client_handle_t client, const char *product_key, const char *device_name, const char *version, int qos, int retain) {
    cJSON *params_node = cJSON_CreateObject();
    cJSON_AddStringToObject(params_node, "version", version);
    char *topic = NULL;
    asprintf(&topic, "/ota/device/inform/%s/%s", product_key, device_name);
    assert(topic);
    publish_alink_msg(client, topic, NULL, params_node, NULL, qos, retain);
    free(topic);
}

static void do_https_ota_upgrade(esp_mqtt_client_handle_t client, const char *product_key, const char *device_name, const char *url, size_t size) {
    esp_http_client_config_t config = {
        .url = url,
        .crt_bundle_attach = esp_crt_bundle_attach,
    };

    esp_https_ota_config_t ota_config = {
        .http_config = &config,
    };

    ESP_LOGI(ALIYUN_TAG, "Attempting to download update from %s", config.url);
    esp_err_t ret = esp_https_ota(&ota_config);
    if (ret == ESP_OK) {
        ESP_LOGI(ALIYUN_TAG, "OTA Succeed, Rebooting...");
        report_progress(client, product_key, device_name, 100, 1, 0);
        esp_restart();
    } else {
        ESP_LOGE(ALIYUN_TAG, "Firmware upgrade failed");
        report_progress(client, product_key, device_name, -1, 1, 0);
        esp_restart();
    }
}

static void request_mqtt_download(esp_mqtt_client_handle_t client, const char *product_key, const char *device_name, const char *file_token, uint32_t stream_id, uint32_t file_id, uint32_t size, uint32_t offset, int qos, int retain) {
        
    cJSON *params_node = cJSON_CreateObject();
    if (file_token) {
        cJSON_AddStringToObject(params_node, "fileToken", file_token);
    }

    cJSON *file_info_node = cJSON_CreateObject();
    cJSON_AddNumberToObject(file_info_node, "streamId", stream_id);
    cJSON_AddNumberToObject(file_info_node, "fileId", file_id);
    cJSON_AddItemToObject(params_node, "fileInfo", file_info_node);
    
    cJSON *file_block_node = cJSON_CreateObject();
    cJSON_AddNumberToObject(file_block_node, "size", size);
    cJSON_AddNumberToObject(file_block_node, "offset", offset);
    cJSON_AddItemToObject(params_node, "fileBlock", file_block_node);

    char *topic = NULL;
    asprintf(&topic, "/sys/%s/%s/thing/file/download", product_key, device_name);
    assert(topic);
    publish_alink_msg(client, topic, NULL, params_node, NULL, qos, retain);
    free(topic);
}

static void send_alink_response(esp_mqtt_client_handle_t client, const char *topic, int code, int qos, int retain) {
    cJSON *root_node = cJSON_CreateObject();
    cJSON_AddNumberToObject(root_node, "id", get_next_alink_id());
    cJSON_AddNumberToObject(root_node, "code", code);
    cJSON_AddObjectToObject(root_node, "data");
    char *msg = cJSON_PrintUnformatted(root_node);
    cJSON_Delete(root_node);
    while (esp_mqtt_client_publish(client, topic, msg, strlen(msg), qos, retain) < 0);
    cJSON_free(msg);
}

static uint16_t crc_ibm(uint8_t const *buffer, size_t len)
{
    /*
     * CRC lookup table for bytes, generating polynomial is 0x8005
     * input: reflexed (LSB first)
     * output: reflexed also...
     */
    const static uint16_t crc_ibm_table[512] = {
        0x0000, 0xc0c1, 0xc181, 0x0140, 0xc301, 0x03c0, 0x0280, 0xc241,
        0xc601, 0x06c0, 0x0780, 0xc741, 0x0500, 0xc5c1, 0xc481, 0x0440,
        0xcc01, 0x0cc0, 0x0d80, 0xcd41, 0x0f00, 0xcfc1, 0xce81, 0x0e40,
        0x0a00, 0xcac1, 0xcb81, 0x0b40, 0xc901, 0x09c0, 0x0880, 0xc841,
        0xd801, 0x18c0, 0x1980, 0xd941, 0x1b00, 0xdbc1, 0xda81, 0x1a40,
        0x1e00, 0xdec1, 0xdf81, 0x1f40, 0xdd01, 0x1dc0, 0x1c80, 0xdc41,
        0x1400, 0xd4c1, 0xd581, 0x1540, 0xd701, 0x17c0, 0x1680, 0xd641,
        0xd201, 0x12c0, 0x1380, 0xd341, 0x1100, 0xd1c1, 0xd081, 0x1040,
        0xf001, 0x30c0, 0x3180, 0xf141, 0x3300, 0xf3c1, 0xf281, 0x3240,
        0x3600, 0xf6c1, 0xf781, 0x3740, 0xf501, 0x35c0, 0x3480, 0xf441,
        0x3c00, 0xfcc1, 0xfd81, 0x3d40, 0xff01, 0x3fc0, 0x3e80, 0xfe41,
        0xfa01, 0x3ac0, 0x3b80, 0xfb41, 0x3900, 0xf9c1, 0xf881, 0x3840,
        0x2800, 0xe8c1, 0xe981, 0x2940, 0xeb01, 0x2bc0, 0x2a80, 0xea41,
        0xee01, 0x2ec0, 0x2f80, 0xef41, 0x2d00, 0xedc1, 0xec81, 0x2c40,
        0xe401, 0x24c0, 0x2580, 0xe541, 0x2700, 0xe7c1, 0xe681, 0x2640,
        0x2200, 0xe2c1, 0xe381, 0x2340, 0xe101, 0x21c0, 0x2080, 0xe041,
        0xa001, 0x60c0, 0x6180, 0xa141, 0x6300, 0xa3c1, 0xa281, 0x6240,
        0x6600, 0xa6c1, 0xa781, 0x6740, 0xa501, 0x65c0, 0x6480, 0xa441,
        0x6c00, 0xacc1, 0xad81, 0x6d40, 0xaf01, 0x6fc0, 0x6e80, 0xae41,
        0xaa01, 0x6ac0, 0x6b80, 0xab41, 0x6900, 0xa9c1, 0xa881, 0x6840,
        0x7800, 0xb8c1, 0xb981, 0x7940, 0xbb01, 0x7bc0, 0x7a80, 0xba41,
        0xbe01, 0x7ec0, 0x7f80, 0xbf41, 0x7d00, 0xbdc1, 0xbc81, 0x7c40,
        0xb401, 0x74c0, 0x7580, 0xb541, 0x7700, 0xb7c1, 0xb681, 0x7640,
        0x7200, 0xb2c1, 0xb381, 0x7340, 0xb101, 0x71c0, 0x7080, 0xb041,
        0x5000, 0x90c1, 0x9181, 0x5140, 0x9301, 0x53c0, 0x5280, 0x9241,
        0x9601, 0x56c0, 0x5780, 0x9741, 0x5500, 0x95c1, 0x9481, 0x5440,
        0x9c01, 0x5cc0, 0x5d80, 0x9d41, 0x5f00, 0x9fc1, 0x9e81, 0x5e40,
        0x5a00, 0x9ac1, 0x9b81, 0x5b40, 0x9901, 0x59c0, 0x5880, 0x9841,
        0x8801, 0x48c0, 0x4980, 0x8941, 0x4b00, 0x8bc1, 0x8a81, 0x4a40,
        0x4e00, 0x8ec1, 0x8f81, 0x4f40, 0x8d01, 0x4dc0, 0x4c80, 0x8c41,
        0x4400, 0x84c1, 0x8581, 0x4540, 0x8701, 0x47c0, 0x4680, 0x8641,
        0x8201, 0x42c0, 0x4380, 0x8341, 0x4100, 0x81c1, 0x8081, 0x4040,
    };

    uint16_t crc = 0x0000;
    uint8_t lut;

    while (len--) {
        lut = (crc ^ (*buffer++)) & 0xFF;
        crc = (crc >> 8) ^ crc_ibm_table[lut];
    }
    return crc;
}

static int verify_mqtt_file_data(uint8_t *data, uint16_t block_size, uint16_t crc16) {
    uint16_t fact_crc16 = crc_ibm(data, block_size);
    if (fact_crc16 != crc16) {
        ESP_LOGI(ALIYUN_TAG, "crc16 mismatch: fact crc16 is 0x%04x, crc16 is 0x%04x", fact_crc16, crc16);
    }
    return fact_crc16 == crc16;
}

static uint8_t get_next_tid() {
    static uint8_t tid;
    return tid++;
}

static void process_thing_model_data(struct mqtt_event_handler_args *args, char *data, size_t len) {
    cJSON *root_node = cJSON_ParseWithLength(data, len);
    if (cJSON_IsObject(root_node)) {
        cJSON *method_node = cJSON_GetObjectItem(root_node, "method");
        if (cJSON_IsString(method_node)) {
            cJSON *params_node = cJSON_GetObjectItem(root_node, "params");
            const char *method = cJSON_GetStringValue(method_node);
            const char service_prefix[] = "thing.service.";
            if (strncmp(service_prefix, method, sizeof service_prefix - 1) == 0) {
                const char *service_name = method + sizeof service_prefix - 1;
                if (strcmp(service_name, "set_onoff") == 0) {
                    if (cJSON_IsObject(params_node)) {
                        cJSON *onoff_node = cJSON_GetObjectItem(params_node, "onoff");
                        cJSON *addr_node = cJSON_GetObjectItem(params_node, "addr");
                        if (cJSON_IsNumber(onoff_node) && cJSON_IsNumber(addr_node)) {
                            uint16_t onoff = cJSON_GetNumberValue(onoff_node);
                            uint16_t addr = cJSON_GetNumberValue(addr_node);
                            
                            esp_ble_mesh_client_common_param_t common = {
                                .opcode = ESP_BLE_MESH_MODEL_OP_GEN_ONOFF_SET_UNACK,
                                .model = onoff_client.model,
                                .ctx = {
                                    .net_idx = 0,
                                    .app_idx = 0,
                                    .addr = addr,
                                    .send_rel = false,
                                },
                                .msg_timeout = 0,
                                .msg_role = ROLE_NODE,
                            };

                            esp_ble_mesh_generic_client_set_state_t set = {
                                .onoff_set = {
                                    .op_en = false,
                                    .onoff = onoff,
                                    .tid = get_next_tid(),
                                },
                            };

                            esp_err_t err = esp_ble_mesh_generic_client_set_state(&common, &set);
                            if (err) {
                                ESP_LOGE(BLEMESH_TAG, "Send Generic OnOff Set Unack failed");
                            }
                        }
                    }
                }
                else if (strcmp(service_name, "set_hsl_color") == 0) {
                    if (cJSON_IsObject(params_node)) {
                        cJSON *hue_node = cJSON_GetObjectItem(params_node, "hue");
                        cJSON *saturation_node = cJSON_GetObjectItem(params_node, "saturation");
                        cJSON *lightness_node = cJSON_GetObjectItem(params_node, "lightness");
                        cJSON *addr_node = cJSON_GetObjectItem(params_node, "addr");
                        if (cJSON_IsNumber(hue_node) && cJSON_IsNumber(saturation_node) && cJSON_IsNumber(lightness_node) && cJSON_IsNumber(addr_node)) {
                            uint16_t hue = cJSON_GetNumberValue(hue_node);
                            uint16_t saturation = cJSON_GetNumberValue(saturation_node);
                            uint16_t lightness = cJSON_GetNumberValue(lightness_node);
                            uint16_t addr = cJSON_GetNumberValue(addr_node);

                            esp_ble_mesh_client_common_param_t common = {
                                .opcode = ESP_BLE_MESH_MODEL_OP_LIGHT_HSL_SET_UNACK,
                                .model = hsl_client.model,
                                .ctx = {
                                    .net_idx = 0,
                                    .app_idx = 0,
                                    .addr = addr,
                                    .send_rel = false,
                                },
                                .msg_timeout = 0,
                                .msg_role = ROLE_NODE,
                            };

                            esp_ble_mesh_light_client_set_state_t set = {
                                .hsl_set = {
                                    .op_en = false,
                                    .hsl_hue = hue,
                                    .hsl_saturation = saturation,
                                    .hsl_lightness = lightness,
                                    .tid = get_next_tid(),
                                },
                            };

                            esp_err_t err = esp_ble_mesh_light_client_set_state(&common, &set);
                            if (err) {
                                ESP_LOGE(BLEMESH_TAG, "Send Light HSL Set Unack failed");
                            }
                        }
                    }
                }
                else if (strcmp(service_name, "property.set") == 0) {
                    if (cJSON_IsObject(params_node)) {
                        cJSON *addr_node = cJSON_GetObjectItem(params_node, "addr");
                        cJSON *net_key_node = cJSON_GetObjectItem(params_node, "net_key");
                        cJSON *app_key_node = cJSON_GetObjectItem(params_node, "app_key");
                        
                        if (cJSON_IsNumber(addr_node)) {
                            // uint16_t addr = cJSON_GetNumberValue(addr_node);
                            // TODO: set node addr
                        }
                        else if (cJSON_IsString(net_key_node)) {
                            const char *net_key_str = cJSON_GetStringValue(net_key_node);
                            if (strlen(net_key_str) == 32) {
                                uint8_t net_key[16];
                                for (int i = 0; i < 16; i++) {
                                    char a = tolower(net_key_str[2 * i]);
                                    char b = tolower(net_key_str[2 * i + 1]);
                                    if (ishex(a) && ishex(b)) {
                                        uint8_t result = tohex(a) << 4;
                                        result |= tohex(b);
                                        net_key[i] = result;
                                    }
                                }
                                esp_err_t err = esp_ble_mesh_node_add_local_net_key(net_key, 0);
                                if (err) {
                                    ESP_LOGE(BLEMESH_TAG, "esp_ble_mesh_node_add_local_net_key: %d", err);
                                }
                            }
                        }
                        else if (cJSON_IsString(app_key_node)) {
                            const char *app_key_str = cJSON_GetStringValue(app_key_node);
                            if (strlen(app_key_str) == 32) {
                                uint8_t app_key[16];
                                for (int i = 0; i < 16; i++) {
                                    char a = tolower(app_key_str[2 * i]);
                                    char b = tolower(app_key_str[2 * i + 1]);
                                    if (ishex(a) && ishex(b)) {
                                        uint8_t result = tohex(a) << 4;
                                        result |= tohex(b);
                                        app_key[i] = result;
                                    }
                                }
                                esp_err_t err = esp_ble_mesh_node_add_local_app_key(app_key, 0, 0);
                                if (err) {
                                    ESP_LOGE(BLEMESH_TAG, "esp_ble_mesh_node_add_local_net_key: %d", err);
                                }
                            }
                        }
                    }
                }
                else if (strcmp(service_name, "reset") == 0) {
                    esp_ble_mesh_node_local_reset();
                }
            }
        }
    }
    cJSON_Delete(root_node);
}

static void request_config(esp_mqtt_client_handle_t client, const char *product_key, const char *device_name, int qos, int retain) {
    char *topic = NULL;
    asprintf(&topic, "/sys/%s/%s/thing/config/get", product_key, device_name);
    assert(topic);

    cJSON *sys_node = cJSON_CreateObject();
    cJSON_AddNumberToObject(sys_node, "ack", 1);

    cJSON *params_node = cJSON_CreateObject();
    cJSON_AddStringToObject(params_node, "configScope", "product");
    cJSON_AddStringToObject(params_node, "getType", "file");

    publish_alink_msg(client, topic, sys_node, params_node, "thing.config.get", qos, retain);
    vPortFree(topic);
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    struct mqtt_event_handler_args *args = handler_args;
    assert(args);

    ESP_LOGD(ALIYUN_TAG, "Event dispatched from event loop base=%s, event_id=%d", base, event_id);
    esp_mqtt_event_handle_t event = event_data;
    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_CONNECTED");
            xEventGroupSetBits(s_wifi_event_group, CONNECTED_BIT);
            report_version(args->client, args->product_key, args->device_name, esp_app_get_description()->version, 1, 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_ota_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_ota_download_reply_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_job_update_reply_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_job_get_reply_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_job_notify_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_bootstrap_notify_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_config_push_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_set_onoff_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_set_hsl_color_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_property_set_topic, 0) < 0);
            while (esp_mqtt_client_subscribe(args->client, args->gateway_property_post_reply_topic, 0) < 0);
            request_config(args->client, args->product_key, args->device_name, 1, 0);
            break;
        }
        case MQTT_EVENT_DISCONNECTED: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DISCONNECTED");
            xEventGroupClearBits(s_wifi_event_group, CONNECTED_BIT);
            break;
        }
        case MQTT_EVENT_SUBSCRIBED: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_SUBSCRIBED, msg_id=%d", event->msg_id);
            break;
        }
        case MQTT_EVENT_UNSUBSCRIBED: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_UNSUBSCRIBED, msg_id=%d", event->msg_id);
            break;
        }
        case MQTT_EVENT_PUBLISHED: {
            // ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_PUBLISHED, msg_id=%d", event->msg_id);
            break;
        }
        case MQTT_EVENT_DATA: {
            if (strncmp(args->gateway_ntp_response_topic, event->topic, event->topic_len - 1) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                ESP_LOGI(ALIYUN_TAG ,"%s", "NTP Response Received");
                do_time_update(event->data, event->data_len);
            }
            else if (strncmp(args->gateway_set_onoff_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                process_thing_model_data(args, event->data, event->data_len);
            }
            else if (strncmp(args->gateway_set_hsl_color_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                process_thing_model_data(args, event->data, event->data_len);
            }
            else if (strncmp(args->gateway_property_set_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                process_thing_model_data(args, event->data, event->data_len);
            }
            else if (strncmp(args->gateway_property_post_reply_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                process_thing_model_data(args, event->data, event->data_len);
            }
            else if (strncmp(args->gateway_ota_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                ESP_LOGI(ALIYUN_TAG, "%s", "OTA Request Received");
                cJSON *root_node = cJSON_ParseWithLength(event->data, event->data_len);
                if (cJSON_IsObject(root_node)) {
                    cJSON *data_node = cJSON_GetObjectItem(root_node, "data");
                    if (cJSON_IsObject(data_node)) {
                        cJSON *version_node = cJSON_GetObjectItem(data_node, "version");
                        cJSON *size_node = cJSON_GetObjectItem(data_node, "size");
                        if (cJSON_IsString(version_node) && cJSON_IsNumber(size_node)) {
                            const char *version = cJSON_GetStringValue(version_node);
                            size_t size = cJSON_GetNumberValue(size_node);
                            if (strcmp(version, esp_app_get_description()->version) == 0) {
                                report_version(args->client, args->product_key, args->device_name, version, 1, 0);
                            }
                            else {
                                cJSON *url_node = cJSON_GetObjectItem(data_node, "url");
                                cJSON *stream_id_node = cJSON_GetObjectItem(data_node, "streamId");
                                cJSON *file_id_node = cJSON_GetObjectItem(data_node, "streamFileId");
                                if (cJSON_IsString(url_node)) {
                                    const char *url = cJSON_GetStringValue(url_node);
                                    do_https_ota_upgrade(args->client, args->product_key, args->device_name, url, size);
                                }
                                else if (cJSON_IsNumber(stream_id_node) && cJSON_IsNumber(file_id_node)) {
                                    args->last_progress = 0;
                                    args->stream_id = cJSON_GetNumberValue(stream_id_node);
                                    args->file_id = cJSON_GetNumberValue(file_id_node);
                                    args->ota_partition = esp_ota_get_next_update_partition(NULL);
                                    ESP_ERROR_CHECK( esp_ota_begin(args->ota_partition, size, &args->ota_handle) );
                                    if (xSemaphoreTake(args->gateway_ota_upgrading_sem, 0) == pdTRUE) {
                                        request_mqtt_download(args->client, args->product_key, args->device_name, NULL, args->stream_id, args->file_id, min(512, (uint32_t)size), 0, 1, 0);
                                    }
                                    else {
                                        ESP_LOGE(ALIYUN_TAG, "Another MQTT upgrade is running");
                                    }
                                }
                            }
                        }
                    }
                }
                cJSON_Delete(root_node);
            }
            else if (strncmp(args->gateway_ota_download_reply_topic, event->topic, event->topic_len) == 0) {
                // ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d", event->topic_len, event->topic, event->msg_id);
                if (event->data_len > 4) {
                    uint16_t json_size = event->data[0];
                    json_size <<= 8;
                    json_size |= event->data[1];
                    // ESP_LOGI(ALIYUN_TAG, "%.*s", json_size, event->data + 2);
                    if (json_size + 2 <= event->data_len) {
                        cJSON *root_node = cJSON_ParseWithLength(event->data + 2, json_size);
                        if (cJSON_IsObject(root_node)) {
                            cJSON *data_node = cJSON_GetObjectItem(root_node, "data");
                            if (cJSON_IsObject(data_node)) {
                                cJSON *block_size_node = cJSON_GetObjectItem(data_node, "bSize");
                                cJSON *block_offset_node = cJSON_GetObjectItem(data_node, "bOffset");
                                cJSON *file_length_node = cJSON_GetObjectItem(data_node, "fileLength");
                                cJSON *file_token_node = cJSON_GetObjectItem(data_node, "fileToken");

                                if (cJSON_IsString(file_token_node)) {
                                    const char *file_token = cJSON_GetStringValue(file_token_node);
                                    ESP_LOGI(ALIYUN_TAG, "file token: %s", file_token);
                                }

                                if (cJSON_IsNumber(block_size_node) && cJSON_IsNumber(block_offset_node) && cJSON_IsNumber(file_length_node)) {
                                    uint32_t block_size = cJSON_GetNumberValue(block_size_node);
                                    uint32_t block_offset = cJSON_GetNumberValue(block_offset_node);
                                    uint32_t file_length = cJSON_GetNumberValue(file_length_node);
                                    if (2 + json_size + block_size + 2 == event->data_len) {
                                        uint8_t *block = (uint8_t*)event->data + 2 + json_size;
                                        uint8_t *block_end = block + block_size;
                                        uint16_t crc16 = block_end[1];
                                        crc16 <<= 8;
                                        crc16 |= block_end[0];
                                        if (verify_mqtt_file_data(block, block_size, crc16)) {
                                            ESP_ERROR_CHECK( esp_ota_write_with_offset(args->ota_handle, block, block_size, block_offset) );

                                            // ESP_LOGI(ALIYUN_TAG, "Upgrading %" PRIu32 "/%" PRIu32 " bytes", block_offset + block_size, file_length);

                                            int progress = (block_offset + block_size) * 100;
                                            progress /= file_length;
                                            if (args->last_progress + 1 <= progress) {
                                                ESP_LOGI(ALIYUN_TAG, "Upgrading: %d%%", progress);
                                                report_progress(args->client, args->product_key, args->device_name, progress, 0, 0);
                                                args->last_progress = progress;
                                            }

                                            if (block_offset + block_size == file_length) {
                                                ESP_ERROR_CHECK( esp_ota_end(args->ota_handle) );
                                                ESP_ERROR_CHECK( esp_ota_set_boot_partition(args->ota_partition) );
                                                ESP_LOGI(ALIYUN_TAG, "Upgraded");
                                                esp_restart();
                                            }
                                            else {
                                                request_mqtt_download(args->client, args->product_key, args->device_name, NULL, args->stream_id, args->file_id, min(512, file_length - block_offset - block_size), block_offset + block_size, 1, 0);
                                            }
                                        }
                                        else {
                                            ESP_LOGE(ALIYUN_TAG, "failed CRC16/IBM verification");
                                            request_mqtt_download(args->client, args->product_key, args->device_name, NULL, args->stream_id, args->file_id, block_size, block_offset, 1, 0);
                                        }
                                    }
                                }
                            }
                        }
                        cJSON_Delete(root_node);
                    }
                }
            }
            else if (strncmp(args->gateway_job_notify_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
            }
            else if (strncmp(args->gateway_job_get_reply_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
            }
            else if (strncmp(args->gateway_job_update_reply_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
            }
            else if (strncmp(args->gateway_bootstrap_notify_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                ESP_ERROR_CHECK( nvs_flash_erase_partition("mqtt") );
                char *topic = NULL;
                asprintf(&topic, "/sys/%s/%s/thing/bootstrap/notify_reply", args->product_key, args->device_name);
                assert(topic);
                send_alink_response(args->client, topic, 200, 1, 0);
                vPortFree(topic);
                esp_restart();
            }
            else if (strncmp(args->gateway_config_push_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
                char *topic = NULL;
                asprintf(&topic, "/sys/%s/%s/thing/config/push_reply", args->product_key, args->device_name);
                assert(topic);
                send_alink_response(args->client, topic, 200, 1, 0);
                vPortFree(topic);
            }
            else if (strncmp(args->gateway_config_get_reply_topic, event->topic, event->topic_len) == 0) {
                ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DATA, topic=%.*s, msg_id=%d, data=%.*s", event->topic_len, event->topic, event->msg_id, event->data_len, event->data);
            }
            break;
        }
        case MQTT_EVENT_ERROR: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_ERROR");
            if (event->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT) {
                ESP_LOGI(ALIYUN_TAG, "Last error code reported from esp-tls: 0x%x", event->error_handle->esp_tls_last_esp_err);
                ESP_LOGI(ALIYUN_TAG, "Last tls stack error number: 0x%x", event->error_handle->esp_tls_stack_err);
                ESP_LOGI(ALIYUN_TAG, "Last captured errno : %d (%s)", event->error_handle->esp_transport_sock_errno,
                         strerror(event->error_handle->esp_transport_sock_errno));
            }
            else if (event->error_handle->error_type == MQTT_ERROR_TYPE_CONNECTION_REFUSED) {
                ESP_LOGI(ALIYUN_TAG, "Connection refused error: 0x%x", event->error_handle->connect_return_code);
            }
            else {
                ESP_LOGW(ALIYUN_TAG, "Unknown error type: 0x%x", event->error_handle->error_type);
            }
            break;
        }
        case MQTT_EVENT_BEFORE_CONNECT: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_BEFORE_CONNECT");
            break;
        }
        case MQTT_EVENT_DELETED: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_EVENT_DELETED");
            break;
        }
        case MQTT_USER_EVENT: {
            ESP_LOGI(ALIYUN_TAG, "MQTT_USER_EVENT");
            break;
        }
        default: {
            break;
        }
    }
}

void app_main(void)
{
    esp_err_t err;

    err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(err);
    ESP_ERROR_CHECK( nvs_flash_init_partition(CONFIG_BLE_MESH_PARTITION_NAME) );
    ESP_ERROR_CHECK( nvs_flash_init_partition(CONFIG_PARTITION) );

    // Initialize TCP/IP network interface aka the esp-netif (should be called only once in application)
    ESP_ERROR_CHECK(esp_netif_init());

    s_wifi_event_group = xEventGroupCreate();

    // Create default event loop that running in background
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    esp_netif_t *sta_netif = esp_netif_create_default_wifi_sta();
    assert(sta_netif);

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK( esp_wifi_init(&cfg) );

    ESP_ERROR_CHECK( esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &event_handler, sta_netif) );
    ESP_ERROR_CHECK( esp_event_handler_register(SC_EVENT, ESP_EVENT_ANY_ID, &event_handler, sta_netif) );

    ESP_ERROR_CHECK( esp_wifi_set_mode(WIFI_MODE_STA) );
    ESP_ERROR_CHECK( esp_wifi_start() );

    // Initialize Ethernet driver
    uint8_t eth_port_cnt = 0;
    esp_eth_handle_t *eth_handles;
    ESP_ERROR_CHECK(example_eth_init(&eth_handles, &eth_port_cnt));

    // Create instance(s) of esp-netif for Ethernet(s)
    if (eth_port_cnt == 1) {
        // Use ESP_NETIF_DEFAULT_ETH when just one Ethernet interface is used and you don't need to modify
        // default esp-netif configuration parameters.
        esp_netif_config_t cfg = ESP_NETIF_DEFAULT_ETH();
        esp_netif_t *eth_netif = esp_netif_new(&cfg);
    
        // Attach Ethernet driver to TCP/IP stack
        esp_eth_netif_glue_handle_t glue = esp_eth_new_netif_glue(eth_handles[0]);
        ESP_ERROR_CHECK(esp_netif_attach(eth_netif, glue));

        // Register user defined event handers
        ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, &eth_event_handler, glue));
    } else {
        // Use ESP_NETIF_INHERENT_DEFAULT_ETH when multiple Ethernet interfaces are used and so you need to modify
        // esp-netif configuration parameters for each interface (name, priority, etc.).
        esp_netif_inherent_config_t esp_netif_config = ESP_NETIF_INHERENT_DEFAULT_ETH();
        esp_netif_config_t cfg_spi = {
            .base = &esp_netif_config,
            .stack = ESP_NETIF_NETSTACK_DEFAULT_ETH
        };
        char if_key_str[10];
        char if_desc_str[10];
        char num_str[3];
        for (int i = 0; i < eth_port_cnt; i++) {
            itoa(i, num_str, 10);
            strcat(strcpy(if_key_str, "ETH_"), num_str);
            strcat(strcpy(if_desc_str, "eth"), num_str);
            esp_netif_config.if_key = if_key_str;
            esp_netif_config.if_desc = if_desc_str;
            esp_netif_config.route_prio -= i*5;
            esp_netif_t *eth_netif = esp_netif_new(&cfg_spi);

            // Attach Ethernet driver to TCP/IP stack
            esp_eth_netif_glue_handle_t glue = esp_eth_new_netif_glue(eth_handles[i]);
            ESP_ERROR_CHECK(esp_netif_attach(eth_netif, glue));

            // Register user defined event handers
            ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, &eth_event_handler, glue));
        }
    }
    
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, ESP_EVENT_ANY_ID, &ip_event_handler, NULL));

    // Start Ethernet driver state machine
    for (int i = 0; i < eth_port_cnt; i++) {
        ESP_ERROR_CHECK(esp_eth_start(eth_handles[i]));
    }

    nvs_handle_t config_nvs_handle;
    ESP_ERROR_CHECK( nvs_open_from_partition(CONFIG_PARTITION, CONFIG_PARTITION, NVS_READONLY, &config_nvs_handle) );
    LOAD_NVS_STR(config_nvs_handle, server_cer);
    LOAD_NVS_STR(config_nvs_handle, client_cer);
    LOAD_NVS_STR(config_nvs_handle, client_key);
    LOAD_NVS_STR(config_nvs_handle, product_key);
    LOAD_NVS_STR(config_nvs_handle, device_name);
    LOAD_NVS_STR(config_nvs_handle, device_secret);
    nvs_close(config_nvs_handle);

    nvs_handle_t mqtt_nvs_handle;
    ESP_ERROR_CHECK( nvs_open("mqtt", NVS_READWRITE, &mqtt_nvs_handle) );
    
    LOAD_OPT_NVS_U16(mqtt_nvs_handle, port);
    LOAD_OPT_NVS_STR(mqtt_nvs_handle, host);
    LOAD_OPT_NVS_STR(mqtt_nvs_handle, username);
    LOAD_OPT_NVS_STR(mqtt_nvs_handle, password);
    LOAD_OPT_NVS_STR(mqtt_nvs_handle, client_id);

    int bootstrap_required = !host_found || !username_found || !password_found || !client_id_found || !port_found;
    ESP_LOGI(ALIYUN_TAG, "bootstrap_required: %d", bootstrap_required);

    if (bootstrap_required) {
        char *post_data = NULL;
        int post_data_len = asprintf(&post_data, "productKey=%s&deviceName=%s", product_key, device_name);
        assert(post_data);

        esp_http_client_config_t config = {
            .host = "iot-auth-global.aliyuncs.com",
            .path = "/auth/bootstrap",
            .transport_type = HTTP_TRANSPORT_OVER_SSL,
            .event_handler = http_event_handler,
            .user_data = (void*)mqtt_nvs_handle,
            .method = HTTP_METHOD_POST,
            .crt_bundle_attach = esp_crt_bundle_attach,
        };
        esp_http_client_handle_t client = esp_http_client_init(&config);
        esp_http_client_set_post_field(client, post_data, post_data_len);
        esp_http_client_set_header(client, "Content-Type", "application/x-www-form-urlencoded");

        while (ESP_OK != esp_http_client_perform(client)) {
            vTaskDelay(pdMS_TO_TICKS(2000));
        }

        esp_http_client_cleanup(client);

        free(post_data);

        assert( nvs_get_str(mqtt_nvs_handle, "host", NULL, &host_size) == ESP_OK
            && nvs_get_u16(mqtt_nvs_handle, "port", &port) == ESP_OK );
        
        char *hmac_source = NULL;
        int hmac_source_len = asprintf(&hmac_source, "clientId%s.%sdeviceName%sproductKey%s", product_key, device_name, device_name, product_key);
        ESP_LOGI(ALIYUN_TAG, "%s", hmac_source);
        assert(hmac_source);
        
        unsigned char hmac[32];
        mbedtls_md_context_t ctx;
        mbedtls_md_init(&ctx);
        assert( mbedtls_md_setup(&ctx, mbedtls_md_info_from_type(MBEDTLS_MD_SHA256), 1) == 0 );
        assert( mbedtls_md_hmac_starts(&ctx, (const unsigned char *)device_secret, strlen(device_secret)) == 0);
        assert( mbedtls_md_hmac_update(&ctx, (const unsigned char *)hmac_source, hmac_source_len) == 0);
        assert( mbedtls_md_hmac_finish(&ctx, hmac) == 0);
        mbedtls_md_free(&ctx);
        free(hmac_source);

        char password[65];
        for (int i = 0; i < 32; i++) {
            sprintf(password + (i * 2), "%02x", hmac[i]);
        }
        ESP_ERROR_CHECK( nvs_set_str(mqtt_nvs_handle, "password", password) );

        char *username = NULL;
        asprintf(&username, "%s&%s", device_name, product_key);
        assert(username);
        ESP_ERROR_CHECK( nvs_set_str(mqtt_nvs_handle, "username", username) );
        free(username);

        char *client_id = NULL;
        asprintf(&client_id, "%s.%s|securemode=%d,signmethod=hmacsha256|", product_key, device_name, 2);
        assert(client_id);
        ESP_ERROR_CHECK( nvs_set_str(mqtt_nvs_handle, "client_id", client_id) );
        free(client_id);

        ESP_ERROR_CHECK( nvs_commit(mqtt_nvs_handle) );
    }

    if (!host_found) {
        RETRY_LOAD_OPT_NVS_STR(mqtt_nvs_handle, host);
    }

    if (!port_found) {
        RETRY_LOAD_OPT_NVS_U16(mqtt_nvs_handle, port);
    }

    if (!username_found) {
        RETRY_LOAD_OPT_NVS_STR(mqtt_nvs_handle, username);
    }

    if (!password_found) {
        RETRY_LOAD_OPT_NVS_STR(mqtt_nvs_handle, password);
    }

    if (!client_id_found) {
        RETRY_LOAD_OPT_NVS_STR(mqtt_nvs_handle, client_id);
    }

    nvs_close(mqtt_nvs_handle);

    err = bluetooth_init();
    if (err) {
        ESP_LOGE(BLEMESH_TAG, "esp32_bluetooth_init failed (err %d)", err);
        return;
    }

    ble_mesh_get_dev_uuid(dev_uuid);

    /* Initialize the Bluetooth Mesh Subsystem */
    err = ble_mesh_init();
    if (err) {
        ESP_LOGE(BLEMESH_TAG, "Bluetooth mesh init failed (err %d)", err);
    }

    const esp_mqtt_client_config_t mqtt_cfg = {
        .broker = {
            .address = {
                .hostname = host,
                .port = port,
                .transport = MQTT_TRANSPORT_OVER_SSL,
            },
            .verification = {
                .certificate = server_cer,
                .certificate_len = server_cer_size,
            },
        },
        .credentials = {
            .client_id = client_id,
            .username = username,
            .authentication = {
                .password = password,
                .certificate = client_cer,
                .certificate_len = client_cer_size,
                .key = client_key,
                .key_len = client_key_size,
            },
        },
    };

    static struct mqtt_event_handler_args args;
    
    while (!(args.client = esp_mqtt_client_init(&mqtt_cfg)));
    asprintf(&args.gateway_ota_topic, "/ota/device/upgrade/%s/%s", product_key, device_name);
    assert(args.gateway_ota_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_ota_topic: %s", args.gateway_ota_topic);

    asprintf(&args.gateway_ntp_response_topic, "/ext/ntp/%s/%s/response", product_key, device_name);
    assert(args.gateway_ntp_response_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_ntp_response_topic: %s", args.gateway_ntp_response_topic);

    asprintf(&args.gateway_ota_download_reply_topic, "/sys/%s/%s/thing/file/download_reply", product_key, device_name);
    assert(args.gateway_ota_download_reply_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_ota_download_reply_topic: %s", args.gateway_ota_download_reply_topic);

    asprintf(&args.gateway_job_notify_topic, "/sys/%s/%s/thing/job/notify", product_key, device_name);
    assert(args.gateway_job_notify_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_job_notify_topic: %s", args.gateway_job_notify_topic);

    asprintf(&args.gateway_job_update_reply_topic, "/sys/%s/%s/thing/job/update_reply", product_key, device_name);
    assert(args.gateway_job_update_reply_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_job_update_reply_topic: %s", args.gateway_job_update_reply_topic);

    asprintf(&args.gateway_job_get_reply_topic, "/sys/%s/%s/thing/job/get_reply", product_key, device_name);
    assert(args.gateway_job_get_reply_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_job_get_reply_topic: %s", args.gateway_job_get_reply_topic);

    asprintf(&args.gateway_bootstrap_notify_topic, "/sys/%s/%s/thing/bootstrap/notify", product_key, device_name);
    assert(args.gateway_bootstrap_notify_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_bootstrap_notify_topic: %s", args.gateway_bootstrap_notify_topic);

    asprintf(&args.gateway_config_push_topic, "/sys/%s/%s/thing/config/push", product_key, device_name);
    assert(args.gateway_config_push_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_config_push_topic: %s", args.gateway_config_push_topic);

    asprintf(&args.gateway_config_get_reply_topic, "/sys/%s/%s/thing/config/get_reply", product_key, device_name);
    assert(args.gateway_config_get_reply_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_config_get_reply_topic: %s", args.gateway_config_get_reply_topic);

    asprintf(&args.gateway_set_onoff_topic, "/sys/%s/%s/thing/service/set_onoff", product_key, device_name);
    assert(args.gateway_set_onoff_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_set_onoff_topic: %s", args.gateway_set_onoff_topic);

    asprintf(&args.gateway_set_hsl_color_topic, "/sys/%s/%s/thing/service/set_hsl_color", product_key, device_name);
    assert(args.gateway_set_hsl_color_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_set_hsl_color_topic: %s", args.gateway_set_hsl_color_topic);

    asprintf(&args.gateway_property_set_topic, "/sys/%s/%s/thing/service/property/set", product_key, device_name);
    assert(args.gateway_property_set_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_property_set_topic: %s", args.gateway_property_set_topic);

    asprintf(&args.gateway_property_post_topic, "/sys/%s/%s/thing/event/property/post", product_key, device_name);
    assert(args.gateway_property_post_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_property_post_topic: %s", args.gateway_property_post_topic);

    asprintf(&args.gateway_property_post_reply_topic, "/sys/%s/%s/thing/event/property/post_reply", product_key, device_name);
    assert(args.gateway_property_post_reply_topic);
    ESP_LOGI(ALIYUN_TAG, "gateway_property_post_reply_topic: %s", args.gateway_property_post_reply_topic);

    args.server_cer = server_cer;
    args.product_key = product_key;
    args.device_name = device_name;
    args.gateway_ota_upgrading_sem = xSemaphoreCreateBinary();
    xSemaphoreGive(args.gateway_ota_upgrading_sem);

    xTaskCreate(time_update_routine, "time_update_task", 2048, &args, tskIDLE_PRIORITY + 1, NULL);

    esp_mqtt_client_register_event(args.client, ESP_EVENT_ANY_ID, mqtt_event_handler, &args);
    esp_mqtt_client_start(args.client);
}
