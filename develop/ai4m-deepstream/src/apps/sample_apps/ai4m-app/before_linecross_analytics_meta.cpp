#include "analytics.h"
#include "gst-nvmessage.h"
#include "gstnvdsmeta.h"
#include "nvbufsurface.h"
#include "nvds_analytics_meta.h"
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
#include "deepstream_test5_app.h"
#include "nvds_obj_encode.h"



void trigger_line_crossing_event(int source_id, int class_id, float x, float y, float w, float h) {
  //  g_print("Triggered line-crossing event: source=%d, class=%d, x=%.2f, y=%.2f, w=%.2f, h=%.2f\n",
    //        source_id, class_id, x, y, w, h);
    // TODO: implement your custom logic here (logging, messaging, etc.)
}


/* custom_parse_nvdsanalytics_meta_data
 * and extract nvanalytics metadata */
bool containsSubstring(const std::vector<std::string>& vec, const std::string& substr) {
    for (const auto& str : vec) {
        if (str.find(substr) != std::string::npos) {
            return true; // Substring found, exit loop
        }
    }
    return false; // Substring not found
}

extern "C" void analytics_custom_parse_nvdsanalytics_meta_data(
    NvDsMetaList *l_user, AnalyticsUserMeta *data, GstBuffer *buf,
    NvDsFrameMeta *frame_meta, float scaleW, float scaleH, TestAppCtx *testAppCtx) {

    NvDsAnalyticsFrameMeta* meta = NULL;
    for (NvDsMetaList *l_user_meta = l_user; l_user_meta != NULL; l_user_meta = l_user_meta->next) {
        NvDsUserMeta* user_meta = (NvDsUserMeta*) (l_user_meta->data);
        if (user_meta->base_meta.meta_type == NVDS_USER_FRAME_META_NVDSANALYTICS) {
            meta = (NvDsAnalyticsFrameMeta*) user_meta->user_meta_data;
            break;
        }
    }

    // // Print all camera machine names and their counts
    // //
    // if (meta) {
    //     for (const auto& kv : meta->objLCCurrCnt) {
    //         if (kv.second == 1) {
    //             //g_print("Camera Machine Name: %s, Count: %lu\n", kv.first.c_str(), kv.second);
    //             //g_print("Frame source_id: %d\n", frame_meta->source_id);
                
    //             for (GList *l_obj = frame_meta->obj_meta_list; l_obj != NULL; l_obj = l_obj->next) {
    //                 NvDsObjectMeta *obj_meta = (NvDsObjectMeta *)(l_obj->data);
    //                 //g_print("  Object class_id: %d, BBox [x=%.2f, y=%.2f, w=%.2f, h=%.2f]\n",
    //                   //      obj_meta->class_id,
    //                   //      obj_meta->rect_params.left,
    //                   //      obj_meta->rect_params.top,
    //                   //      obj_meta->rect_params.width,
    //                   //      obj_meta->rect_params.height);
    
    //                 for (NvDsMetaList *l_class = obj_meta->classifier_meta_list; l_class != NULL; l_class = l_class->next) {
    //                     NvDsClassifierMeta *classifier_meta = (NvDsClassifierMeta *)(l_class->data);
    //                    // g_print("    Classifier Component ID: %d\n", classifier_meta->unique_component_id);
    //                 }
    //             }
    //         }
    //     }
    // }
    

    std::string taping = "tapping_outfeed";
    std::string infeed = "_infeed";
    GList *l;
    GstMapInfo inmap = GST_MAP_INFO_INIT;

    if (!gst_buffer_map(buf, &inmap, GST_MAP_READ)) {
        GST_ERROR("input buffer mapinfo failed");
    }
    gst_buffer_unmap(buf, &inmap);

    for (l = frame_meta->obj_meta_list; l != NULL; l = l->next) {
        NvDsObjectMeta *obj_meta = (NvDsObjectMeta *)(l->data);
        gboolean taping_flag = false;
        gboolean infeed_flag = false;

        obj_meta->rect_params.border_color.red   = 1.0;
        obj_meta->rect_params.border_color.green = 1.0;
        obj_meta->rect_params.border_color.blue  = 0.0;
        obj_meta->rect_params.border_color.alpha = 1.0;

        if (meta->objLCCurrCnt["mc17_cld"] || meta->objLCCurrCnt["mc18_cld"] ||
            meta->objLCCurrCnt["mc19_cld"] || meta->objLCCurrCnt["mc20_cld"] ||
            meta->objLCCurrCnt["mc21_cld"] || meta->objLCCurrCnt["mc22_cld"]) {

            for (GList *l_inner = frame_meta->obj_meta_list; l_inner != NULL; l_inner = l_inner->next) {
                NvDsObjectMeta *obj_meta_inner = (NvDsObjectMeta *)(l_inner->data);

                gint class_id = obj_meta_inner->class_id;
                float x = obj_meta_inner->rect_params.left * scaleW;
                float y = obj_meta_inner->rect_params.top * scaleH;
                float w = obj_meta_inner->rect_params.width * scaleW;
                float h = obj_meta_inner->rect_params.height * scaleH;

                for (NvDsMetaList *l_user_meta = obj_meta_inner->obj_user_meta_list; l_user_meta != NULL; l_user_meta = l_user_meta->next) {
                    NvDsUserMeta *user_meta = (NvDsUserMeta *)(l_user_meta->data);

                    if (user_meta->base_meta.meta_type == NVDS_USER_OBJ_META_NVDSANALYTICS) {
                        NvDsAnalyticsObjInfo *user_meta_data = (NvDsAnalyticsObjInfo *)user_meta->user_meta_data;

                        if (user_meta_data->lcStatus.size() > 0) {
                            //g_print("Line-crossing detected for object class %d at [x=%.2f, y=%.2f, w=%.2f, h=%.2f]\n" ,class_id, x, y, w, h);

                            trigger_line_crossing_event(frame_meta->source_id + 1, class_id, x, y, w, h);
                        }
                    }
                }
            }
        }

        if (g_list_length(obj_meta->obj_user_meta_list) == 0) {
            nvds_clear_classifier_meta_list(obj_meta, obj_meta->classifier_meta_list);
        }

        for (NvDsMetaList *l_user_meta = obj_meta->obj_user_meta_list; l_user_meta != NULL; l_user_meta = l_user_meta->next) {
            NvDsUserMeta *user_meta = (NvDsUserMeta *) (l_user_meta->data);

            if (user_meta->base_meta.meta_type == NVDS_USER_OBJ_META_NVDSANALYTICS) {
                NvDsAnalyticsObjInfo *user_meta_data = (NvDsAnalyticsObjInfo *)user_meta->user_meta_data;

                if (user_meta_data->roiStatus.size() > 0) {
                    if (frame_meta->source_id == 3) {
                        if (containsSubstring(user_meta_data->roiStatus, taping)) {
                            taping_flag = true;
                        } else {
                            taping_flag = false;
                            nvds_clear_classifier_meta_list(obj_meta, obj_meta->classifier_meta_list);
                        }
                    }

                    if (frame_meta->source_id != 3) {
                        if (containsSubstring(user_meta_data->roiStatus, infeed)) {
                            infeed_flag = true;
                        } else {
                            nvds_clear_classifier_meta_list(obj_meta, obj_meta->classifier_meta_list);
                            infeed_flag = false;
                        }
                    }
                } else {
                    nvds_clear_classifier_meta_list(obj_meta, obj_meta->classifier_meta_list);
                }
            } else {
                nvds_clear_classifier_meta_list(obj_meta, obj_meta->classifier_meta_list);
            }
        }

        for (NvDsMetaList *l_class = obj_meta->classifier_meta_list; l_class != NULL; l_class = l_class->next) {
            NvDsClassifierMeta *classifier_meta = (NvDsClassifierMeta *)(l_class->data);

            if (classifier_meta->unique_component_id == 2 && taping_flag) {
                nvds_remove_classifier_meta_from_obj(obj_meta, classifier_meta);
                break;
            }

            if (classifier_meta->unique_component_id == 3 && infeed_flag) {
                nvds_remove_classifier_meta_from_obj(obj_meta, classifier_meta);
                break;
            }
        }
    }
}


