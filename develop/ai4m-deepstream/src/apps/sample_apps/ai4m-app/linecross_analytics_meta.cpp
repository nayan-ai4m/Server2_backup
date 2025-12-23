//final 

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

/* custom_parse_nvdsanalytics_meta_data
 * and extract nvanalytics metadata */
extern "C" void analytics_custom_parse_nvdsanalytics_meta_data(
    NvDsMetaList *l_user, AnalyticsUserMeta *data, GstBuffer *buf,
    NvDsFrameMeta *frame_meta, float scaleW, float scaleH,TestAppCtx *testAppCtx) {
  NvDsUserMeta *user_meta = (NvDsUserMeta *)l_user->data;
  /* convert to  metadata */
  NvDsAnalyticsFrameMeta *meta = (NvDsAnalyticsFrameMeta *)user_meta->user_meta_data;
  NvDsObjectMeta *obj_meta = NULL;

  //gchar *dummy = (gchar *)"none";
  GList *l;
  GstMapInfo inmap = GST_MAP_INFO_INIT;
  if (!gst_buffer_map(buf, &inmap, GST_MAP_READ)) {
    GST_ERROR("input buffer mapinfo failed");
  }
  NvBufSurface *ip_surf = (NvBufSurface *)inmap.data;
  gst_buffer_unmap(buf, &inmap);
  
  // Determine which MC condition is triggered and get corresponding zone
  std::string mc_prefix = "";
  std::string zone_name = "";
  
  if (meta->objLCCurrCnt["mc17_cld"]) {
    mc_prefix = "mc17_";
    zone_name = "mc17_infeed_jamming";
  } else if (meta->objLCCurrCnt["mc18_cld"]) {
    mc_prefix = "mc18_";
    zone_name = "mc18_infeed_jamming";
  } else if (meta->objLCCurrCnt["mc19_cld"]) {
    mc_prefix = "mc19_";
    zone_name = "mc19_infeed_jamming";
  } else if (meta->objLCCurrCnt["mc20_cld"]) {
    mc_prefix = "mc20_";
    zone_name = "mc20_infeed_jamming";
  } else if (meta->objLCCurrCnt["mc21_cld"]) {
    mc_prefix = "mc21_";
    zone_name = "mc21_infeed_jamming";
  } else if (meta->objLCCurrCnt["mc22_cld"]) {
    mc_prefix = "mc22_";
    zone_name = "mc22_infeed_jamming";
  }

  if (!mc_prefix.empty() && !zone_name.empty())
  {
	  for (l = frame_meta->obj_meta_list; l != NULL; l = l->next) {
    	 		obj_meta = (NvDsObjectMeta *)(l->data);
			gint class_id = obj_meta->class_id;
			float x,y,w,h;
			x = obj_meta->rect_params.left * scaleW ;
    			y =  obj_meta->rect_params.top * scaleH ;
    			w = obj_meta->rect_params.width * scaleW;
    			h = obj_meta->rect_params.height * scaleH;
			
			bool object_in_zone = false;
			bool line_crossed = false;
			
			for (NvDsMetaList *l_user_meta = obj_meta->obj_user_meta_list; l_user_meta != NULL;
                    		l_user_meta = l_user_meta->next) {
               	 			NvDsUserMeta *user_meta = (NvDsUserMeta *) (l_user_meta->data);
                			if(user_meta->base_meta.meta_type == NVDS_USER_OBJ_META_NVDSANALYTICS){
                        			NvDsAnalyticsObjInfo * user_meta_data = (NvDsAnalyticsObjInfo *)user_meta->user_meta_data;
                        		
                        		// Check if object crossed the line
                        		if (user_meta_data->lcStatus.size() > 0) {
                        			line_crossed = true;
                        		}
                        		
                        		// Check if object is in the corresponding zone
                        		if (user_meta_data->roiStatus.size() > 0) {
                        			for (auto const& roiStatus : user_meta_data->roiStatus) {
                        				if (roiStatus == zone_name) {
                        					object_in_zone = true;
                        					g_print("Object %ld is in zone: %s\n", obj_meta->object_id, zone_name.c_str());
                        					break;
                        				}
                        			}
                        		}
                			}
        			}
        		
        		// Only save image if both conditions are met: line crossed AND object in zone
        		if (line_crossed && object_in_zone) {
        			g_print("Saving image: Line crossed and object in zone %s\n", zone_name.c_str());
        			
					NvDsObjEncUsrArgs userData = {0};
                    userData.isFrame = 1;
                    std::time_t t = std::chrono::system_clock::to_time_t(
                    std::chrono::system_clock::now());
                    auto duration = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch();
                    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    std::stringstream time_stamp;
                    time_stamp << std::put_time(std::localtime(&t), "%Y_%m_%dT%H_%M_%S_")<< std::setfill('0') << std::setw(3) << millis;
                    snprintf(userData.fileNameImg,FILE_NAME_SIZE,"/home/ai4m/develop/data/cam/%sraw_%d_%f_%f_%f_%f_%s.jpeg",mc_prefix.c_str(),class_id,x,y,x+w,y+h,time_stamp.str().c_str());
                    userData.saveImg = true;
                    userData.scaleImg = false;
                    userData.scaledWidth = 0;
                    userData.objNum = 2;
                    userData.scaledHeight = 0;
                    userData.quality = 95;
                    frame_meta->filename = g_strdup(userData.fileNameImg);
                    nvds_obj_enc_process(testAppCtx->obj_ctx_handle, &userData, ip_surf,obj_meta, frame_meta);
                    
                    g_print("filename %s \n",frame_meta->filename);
        		} else {
        			if (!line_crossed) {
        				g_print("Object %ld: Line not crossed\n", obj_meta->object_id);
        			}
        			if (!object_in_zone) {
        				g_print("Object %ld: Not in zone %s\n", obj_meta->object_id, zone_name.c_str());
        			}
        		}
		}

	nvds_obj_enc_finish (testAppCtx->obj_ctx_handle);
  }
}

