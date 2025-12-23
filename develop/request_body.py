def build_telemetry_request_body(entity_group_id):
    """ Builds the request body for fetching device telemetry data. """
    return {
        "entityFilter": {
            "type": "entityGroup",
            "groupType": "DEVICE",
            "entityGroup": entity_group_id
        },
        "entityFields": [
            {"type": "ENTITY_FIELD", "key": "name"},
            {"type": "ENTITY_FIELD", "key": "label"},
            {"type": "ENTITY_FIELD", "key": "type"}
        ],
        "latestValues": [
            {"type": "TIME_SERIES", "key": "x_rms_vel"},
            {"type": "TIME_SERIES", "key": "x_rms_acl"},
            {"type": "TIME_SERIES", "key": "y_rms_vel"},
            {"type": "TIME_SERIES", "key": "y_rms_acl"},
            {"type": "TIME_SERIES", "key": "z_rms_vel"},
            {"type": "TIME_SERIES", "key": "z_rms_acl"},
            {"type": "TIME_SERIES", "key": "temp_c"},
            {"type": "TIME_SERIES", "key": "SPL_dB"},
            {"type": "TIME_SERIES", "key": "LED_status"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Operational_Velocity_AXIS_X"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Caution_Velocity_AXIS_X"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Warning_Velocity_AXIS_X"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Operational_Velocity_AXIS_Y"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Caution_Velocity_AXIS_Y"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Warning_Velocity_AXIS_Y"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Operational_Velocity_AXIS_Z"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Caution_Velocity_AXIS_Z"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Warning_Velocity_AXIS_Z"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Operational_Val_Temperature"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Caution_Val_Temperature"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Warning_Val_Temperature"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Operational_Val_Noise"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Caution_Val_Noise"},
            {"type": "CLIENT_ATTRIBUTE", "key": "Warning_Val_Noise"},
            {"type": "SERVER_ATTRIBUTE", "key": "Sensor_Location"}
        ],
        "pageLink": {
            "dynamic": True,
            "page": 0,
            "pageSize": 2000,
            "sortOrder": {
                "key": {
                    "key": "name",
                    "type": "ENTITY_FIELD"
                },
                "direction": "ASC"
            }
        }
    }

