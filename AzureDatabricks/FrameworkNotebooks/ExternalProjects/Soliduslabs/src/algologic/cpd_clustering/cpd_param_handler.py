# Databricks notebook source
from typing import Dict, List, Any


# config-manager returns a rather complex structure for threshold/parameter information - all we need
# are the "days-back" values for that parameter. We return a dict that maps [segment_id: days_back]
def convert_model_info_to_cpd_time_back(solidus_client: str, segment_models: List[Dict[str, Any]]) -> Dict[str, int]:
    return {
        segment_model.get('id'): _extract_time_back_from_seg_model_dtos(solidus_client, segment_model)
        for segment_model in segment_models
    }


def _extract_time_back_from_seg_model_dtos(solidus_client: str, segment_model: Dict[str, Any]) -> int:
    # find the CPD model from all models within the segment
    cpd_model = [
        model for model in segment_model.get('models')
        if model.get('name') == 'PROFILE_DEVIATION' and model.get('params')
    ]
    if not cpd_model:
        print('ModelDto does not exist with "name" PROFILE_DEVIATION '
              f'for solidus_client: {solidus_client}, segment: {cpd_model.get("id")}')
        return None

    baseline_param_dict_list = [param for param in cpd_model[0].get('params') if param.get('name') == 'BASELINE']

    if not baseline_param_dict_list:
        print('baseline_param param dict list does not exist with "name" BASELINE '
              f'for solidus_client: {solidus_client}, segment: {cpd_model.get("id")}')
        return None

    days_back_period_dict = {
        k: v for k, v in baseline_param_dict_list[0].items() if baseline_param_dict_list[0].get('values')
    }

    if not days_back_period_dict or not days_back_period_dict.get('values'):
        print('days_back_period_values dict does not exist with "name" values '
              f'for solidus_client: {solidus_client}, segment: {cpd_model.get("id")}')
        return None

    days_back_period_values = [
        value_dict for value_dict in days_back_period_dict.get('values') if value_dict.get('BASELINE_PERIOD')
    ]

    if not days_back_period_values:
        print('value dictionary not exist with "name" BASELINE_PERIOD '
              f'for solidus_client: {solidus_client}, segment: {cpd_model.get("id")}')
        return None

    return int(days_back_period_values[0].get('BASELINE_PERIOD').get('value'))
