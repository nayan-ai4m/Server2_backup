
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
    NvDsFrameMeta *frame_meta, float scaleW, float scaleH,TestAppCtx *testAppCtx) {
  //NvDsUserMeta *user_meta = (NvDsUserMeta *)l_user->data;
  /* convert to  metadata */
  //NvDsAnalyticsFrameMeta *meta = (NvDsAnalyticsFrameMeta *)user_meta->user_meta_data;
  NvDsObjectMeta *obj_meta = NULL;
  std::string taping = "tapping_outfeed";
  std::string lh_infeed = "infeed_lh";
  std::string rh_infeed = "infeed_rh";
  //gchar *dummy = (gchar *)"none";
  GList *l;
  //NvDsClassifierMetaList *temp_list ;
  GstMapInfo inmap = GST_MAP_INFO_INIT;
  if (!gst_buffer_map(buf, &inmap, GST_MAP_READ)) {
    GST_ERROR("input buffer mapinfo failed");
  }
  //NvBufSurface *ip_surf = (NvBufSurface *)inmap.data;
  gst_buffer_unmap(buf, &inmap);
	  for (l = frame_meta->obj_meta_list; l != NULL; l = l->next) {
    	 		obj_meta = (NvDsObjectMeta *)(l->data);
			obj_meta->rect_params.border_color.red = 1.0;
                        obj_meta->rect_params.border_color.green = 1.0;
                        obj_meta->rect_params.border_color.blue = 0.0;
                        obj_meta->rect_params.border_color.alpha = 1.0;
			//gint class_id = obj_meta->class_id;
			gboolean lh_infeed_flag = false;
			gboolean rh_infeed_flag = false;
			gboolean taping_flag = false;
			//gboolean rm_flag = false;
			//float x,y,w,h;
			//x = obj_meta->rect_params.left * scaleW ;
    			//y =  obj_meta->rect_params.top * scaleH ;
    			//w = obj_meta->rect_params.width * scaleW;
    			//h = obj_meta->rect_params.height * scaleH;
			if ( g_list_length(obj_meta->obj_user_meta_list) == 0 ){
			nvds_clear_classifier_meta_list(obj_meta,obj_meta->classifier_meta_list);
			}
			for (NvDsMetaList *l_user_meta = obj_meta->obj_user_meta_list; l_user_meta != NULL;
                    		l_user_meta = l_user_meta->next) {
               	 			NvDsUserMeta *user_meta = (NvDsUserMeta *) (l_user_meta->data);
                			if(user_meta->base_meta.meta_type == NVDS_USER_OBJ_META_NVDSANALYTICS){
                        			NvDsAnalyticsObjInfo * user_meta_data = (NvDsAnalyticsObjInfo *)user_meta->user_meta_data;
						if (user_meta_data->roiStatus.size() > 0){
							if (frame_meta->source_id ==3 ){
								if (containsSubstring(user_meta_data->roiStatus, taping))
								{
								    taping_flag = true;
								    lh_infeed_flag = false;
								    rh_infeed_flag = false;

								}
								else {
									taping_flag = false;

									nvds_clear_classifier_meta_list(obj_meta,obj_meta->classifier_meta_list);
								}
							}
							if (frame_meta->source_id < 3 ){
                                                                if (containsSubstring(user_meta_data->roiStatus, lh_infeed))
                                                                {
									g_print("object id %ld left \n",obj_meta->object_id);
									lh_infeed_flag = true;
									rh_infeed_flag = false;

                                                                }
								if (containsSubstring(user_meta_data->roiStatus,rh_infeed))
								{
									rh_infeed_flag = true;
									lh_infeed_flag = false;
									g_print("object id %ld right \n",obj_meta->object_id);
								}

                                                        }


						}
						else {
                                        nvds_clear_classifier_meta_list(obj_meta,obj_meta->classifier_meta_list);
                                        }



                			}
					else {
					nvds_clear_classifier_meta_list(obj_meta,obj_meta->classifier_meta_list);
					}
        			}
			
			for (NvDsMetaList *l_class = obj_meta->classifier_meta_list; l_class != NULL; l_class = l_class->next) {
		                NvDsClassifierMeta *classifier_meta = (NvDsClassifierMeta *)(l_class->data);
				if (lh_infeed_flag == true   && classifier_meta->unique_component_id != 2)
				{	
					
					g_print("removed object %ld from left with id %d \n",obj_meta->object_id,classifier_meta->unique_component_id);
					//nvds_remove_label_info_meta_from_classifier(classifier_meta,label_info);
					nvds_remove_classifier_meta_from_obj(obj_meta,classifier_meta);
					break;
				}
				if (rh_infeed_flag == true  && classifier_meta->unique_component_id != 4)
				{
					g_print("removed object %ld from right with id %d \n",obj_meta->object_id,classifier_meta->unique_component_id);
					nvds_remove_classifier_meta_from_obj(obj_meta,classifier_meta);
                                        break;
                                }
				if (taping_flag == true && classifier_meta->unique_component_id != 3 )
                                {
					//g_print("removed random taping \n");
					nvds_remove_classifier_meta_from_obj(obj_meta,classifier_meta);
                                        break;
                                }
				/*
				if (rm_flag)
				{	
				nvds_remove_classifier_meta_from_obj(obj_meta,classifier_meta);
				break;
				}
				*/
			}
			}

}

