
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
  std::string infeed = "_infeed";
  //gchar *dummy = (gchar *)"none";
  GList *l;
  GstMapInfo inmap = GST_MAP_INFO_INIT;
  if (!gst_buffer_map(buf, &inmap, GST_MAP_READ)) {
    GST_ERROR("input buffer mapinfo failed");
  }
  //NvBufSurface *ip_surf = (NvBufSurface *)inmap.data;
  gst_buffer_unmap(buf, &inmap);
	  for (l = frame_meta->obj_meta_list; l != NULL; l = l->next) {
    	 		obj_meta = (NvDsObjectMeta *)(l->data);
			 gboolean taping_flag = false;
  			gboolean infeed_flag = false;
			obj_meta->rect_params.border_color.red = 1.0;
                        obj_meta->rect_params.border_color.green = 1.0;
                        obj_meta->rect_params.border_color.blue = 0.0;
                        obj_meta->rect_params.border_color.alpha = 1.0;

			//gint class_id = obj_meta->class_id;
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
								}
								else {
									taping_flag = false;
									nvds_clear_classifier_meta_list(obj_meta,obj_meta->classifier_meta_list);
								}
							}
							if (frame_meta->source_id != 3 ){
                                                                if (containsSubstring(user_meta_data->roiStatus, infeed))
                                                                {
									infeed_flag = true;
								
                                                                }
								else {
									nvds_clear_classifier_meta_list(obj_meta,obj_meta->classifier_meta_list);
									infeed_flag=false;
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
				if (classifier_meta->unique_component_id == 2 && taping_flag == true){
					nvds_remove_classifier_meta_from_obj(obj_meta,classifier_meta);
					break;
				}
				 if (classifier_meta->unique_component_id == 3 && infeed_flag == true){
                                        nvds_remove_classifier_meta_from_obj(obj_meta,classifier_meta);
                                        break;
                                }
			
			}
			/*
			for (NvDsMetaList *l = obj_meta->classifier_meta_list; l != NULL; l = l->next) {
     				NvDsClassifierMeta *classifierMeta = (NvDsClassifierMeta *) (l->data);
        			if (classifierMeta->unique_component_id == 2){
                			for (NvDsMetaList *l_label = classifierMeta->label_info_list; l_label != NULL; l_label = l_label->next) {
                    				NvDsLabelInfo *label_info = (NvDsLabelInfo *)(l_label->data);
                    				//g_print("classid %d , class_label %s\n ",label_info->result_class_id,label_info->result_label);
                    					if (label_info->result_class_id == 0 ){
                       						obj_meta->rect_params.border_color.red = 1.0;
                        					obj_meta->rect_params.border_color.green = 0.0;
                        					obj_meta->rect_params.border_color.blue = 0.0;
                        					obj_meta->rect_params.border_color.alpha = 1.0;
                    						}
                    					else {
                        					obj_meta->rect_params.border_color.red = 0.0;
                        					obj_meta->rect_params.border_color.green = 1.0;
                        					obj_meta->rect_params.border_color.blue = 0.0;
                        					obj_meta->rect_params.border_color.alpha = 1.0;
                    					     }
        				}


  				}
				 if (classifierMeta->unique_component_id ==3){
                                        for (NvDsMetaList *l_label = classifierMeta->label_info_list; l_label != NULL; l_label = l_label->next) {
                                                NvDsLabelInfo *label_info = (NvDsLabelInfo *)(l_label->data);
                                                //g_print("classid %d , class_label %s\n ",label_info->result_class_id,label_info->result_label);
                                                        if (label_info->result_class_id == 0 ){
                                                                obj_meta->rect_params.border_color.red = 1.0;
                                                                obj_meta->rect_params.border_color.green = 0.0;
                                                                obj_meta->rect_params.border_color.blue = 0.0;
                                                                obj_meta->rect_params.border_color.alpha = 1.0;
                                                                }
                                                        else {
                                                                obj_meta->rect_params.border_color.red = 0.0;
                                                                obj_meta->rect_params.border_color.green = 1.0;
                                                                obj_meta->rect_params.border_color.blue = 0.0;
                                                                obj_meta->rect_params.border_color.alpha = 1.0;
                                                             }
                                        }


                                }

  			}*/



			}

}

