#include "analytics.h"
#include "gst-nvmessage.h"
#include "gstnvdsmeta.h"
#include "nvbufsurface.h"
#include "nvds_analytics_meta.h"
#include "deepstream_test5_app.h"
#include "nvds_obj_encode.h"

#include <bits/stdc++.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <glib.h>
#include <gst/gst.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

/**
 * @brief Custom parser for NvDsAnalytics metadata. Extracts line-crossing and zone-entry
 *        information and saves an image when an object both crosses a line and is in a
 *        configured zone.
 */
extern "C" void analytics_custom_parse_nvdsanalytics_meta_data(
    NvDsMetaList *l_user,
    AnalyticsUserMeta *data,
    GstBuffer *buf,
    NvDsFrameMeta *frame_meta,
    float scaleW,
    float scaleH,
    TestAppCtx *testAppCtx)
{
    NvDsUserMeta *user_meta = (NvDsUserMeta *)l_user->data;
    if (!user_meta)
        return;

    NvDsAnalyticsFrameMeta *meta = (NvDsAnalyticsFrameMeta *)user_meta->user_meta_data;
    if (!meta)
        return;

    GstMapInfo inmap = GST_MAP_INFO_INIT;
    if (!gst_buffer_map(buf, &inmap, GST_MAP_READ)) {
        GST_ERROR("Failed to map input buffer.");
        return;
    }

    NvBufSurface *ip_surf = (NvBufSurface *)inmap.data;
    gst_buffer_unmap(buf, &inmap);

    // Determine which MC condition is triggered
    std::string mc_prefix = "";
    std::string zone_name = "";
    std::string save_path = "/home/ai4m/develop/data/mat_cam/";

    if (meta->objLCCurrCnt["pre_taping_l3"]) {
        mc_prefix = "pre_taping_l3_";
        zone_name = "pre_tap_l3";
    } else if (meta->objLCCurrCnt["pre_taping_l4"]) {
        mc_prefix = "pre_taping_l4_";
        zone_name = "pre_tap_l4";
    }

    if (mc_prefix.empty())
        return;

    // Iterate over all detected objects in the frame
    for (GList *l = frame_meta->obj_meta_list; l != NULL; l = l->next) {
        NvDsObjectMeta *obj_meta = (NvDsObjectMeta *)(l->data);
        if (!obj_meta)
            continue;

        gint class_id = obj_meta->class_id;

        float x = obj_meta->rect_params.left * scaleW;
        float y = obj_meta->rect_params.top * scaleH;
        float w = obj_meta->rect_params.width * scaleW;
        float h = obj_meta->rect_params.height * scaleH;

        bool object_in_zone = false;
        bool line_crossed = false;

        // Check analytics metadata attached to object
        for (NvDsMetaList *l_user_meta = obj_meta->obj_user_meta_list;
             l_user_meta != NULL;
             l_user_meta = l_user_meta->next)
        {
            NvDsUserMeta *user_meta = (NvDsUserMeta *)(l_user_meta->data);
            if (user_meta && user_meta->base_meta.meta_type == NVDS_USER_OBJ_META_NVDSANALYTICS) {
                NvDsAnalyticsObjInfo *user_meta_data =
                    (NvDsAnalyticsObjInfo *)user_meta->user_meta_data;

                if (user_meta_data) {
                    // Line crossing detected
                    if (!user_meta_data->lcStatus.empty()) {
                        line_crossed = true;
                    }

                    // Object in zone
                    for (const auto &roiStatus : user_meta_data->roiStatus) {
                        if (roiStatus == zone_name) {
                            object_in_zone = true;
                            g_print("Object %ld entered zone: %s\n",
                                    obj_meta->object_id, zone_name.c_str());
                            break;
                        }
                    }
                }
            }
        }

        // Save image if both conditions are met
        if (line_crossed && object_in_zone) {
            g_print("Saving frame: Line crossed and object in zone %s\n",
                    zone_name.c_str());

            NvDsObjEncUsrArgs userData = {0};
            userData.isFrame = 1;
            userData.saveImg = true;
            userData.scaleImg = false;
            userData.objNum = 2;
            userData.scaledWidth = 0;
            userData.scaledHeight = 0;
            userData.quality = 95;

            // Timestamp for filename
            auto now = std::chrono::system_clock::now();
            std::time_t t = std::chrono::system_clock::to_time_t(now);
            auto duration = std::chrono::time_point_cast<std::chrono::milliseconds>(now)
                                .time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

            std::stringstream time_stamp;
            time_stamp << std::put_time(std::localtime(&t), "%Y_%m_%dT%H_%M_%S_")
                       << std::setfill('0') << std::setw(3) << (millis % 1000);

            // Construct filename
            snprintf(userData.fileNameImg, FILE_NAME_SIZE,
                     "%s%sraw_%d_%.2f_%.2f_%.2f_%.2f_%s.png",
                     save_path.c_str(),
                     mc_prefix.c_str(),
                     class_id,
                     x, y, x + w, y + h,
                     time_stamp.str().c_str());

            frame_meta->filename = g_strdup(userData.fileNameImg);
            nvds_obj_enc_process(testAppCtx->obj_ctx_handle, &userData,
                                 ip_surf, obj_meta, frame_meta);

            g_print("Saved image: %s\n", frame_meta->filename);
        } else {
            if (!line_crossed)
                g_print("Object %ld did NOT cross line\n", obj_meta->object_id);
            if (!object_in_zone)
                g_print("Object %ld not in zone %s\n",
                        obj_meta->object_id, zone_name.c_str());
        }
    }

    // Finalize image encoding
    nvds_obj_enc_finish(testAppCtx->obj_ctx_handle);
}

