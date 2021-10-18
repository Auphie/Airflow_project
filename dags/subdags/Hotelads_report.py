#!/usr/bin/env python
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
import csv
import google.ads.google_ads.client
import json
import pandas as pd
import numpy as np

_DEFAULT_PAGE_SIZE = 1000


def main(client, customer_id, page_size):
    ga_service = client.get_service('GoogleAdsService', version='v2')

    query = ('SELECT campaign.name, campaign.advertising_channel_type, '
             'segments.device, '
             'ad_group.name, ad_group.status, metrics.impressions, '
             'segments.date, '
             'segments.hotel_date_selection_type, '
             'segments.hotel_price_bucket, '
             'segments.hotel_booking_window_days,'
             'segments.partner_hotel_id, '
             'segments.geo_target_country, '
             'segments.hotel_country, '
             'segments.hotel_check_in_date, '
             'segments.hotel_length_of_stay '
             'FROM hotel_performance_view '
             'WHERE segments.date = "2020-01-27" '
             'AND ad_group.status = \'ENABLED\' '
             'ORDER BY metrics.impressions DESC '
            )

    response = ga_service.search(customer_id, query, page_size=page_size)


    try:
        records = []
        for row in response:
            data = {}
#            data['campaign'] = row.campaign
#            data['ad_group'] = row.ad_group

            data['device'] = row.segments.device
            data['hotel_id'] = row.segments.partner_hotel_id.value
            data['search_date'] = row.segments.date.value
            data['check_in'] = row.segments.hotel_check_in_date.value
            data['len_of_stay'] = row.segments.hotel_length_of_stay.value
            data['in_adv_days'] = row.segments.hotel_booking_window_days.value
            data['price_level'] = row.segments.hotel_price_bucket
            data['date_set'] = row.segments.hotel_date_selection_type
            data['user_country'] = row.segments.geo_target_country.value
            data['impressions'] = row.metrics.impressions.value

            records.append(data)

        df = pd.DataFrame.from_records(records)
        return df

    except google.ads.google_ads.errors.GoogleAdsException as ex:
        print('Request with ID "%s" failed with status "%s" and includes the '
              'following errors:' % (ex.request_id, ex.error.code().name))
        for error in ex.failure.errors:
            print('\tError with message "%s".' % error.message)
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print('\t\tOn field: %s' % field_path_element.field_name)
        sys.exit(1)

if __name__ == '__main__':
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = (google.ads.google_ads.client.GoogleAdsClient
                         .load_from_storage())

    parser = argparse.ArgumentParser(
        description=('Retrieves Hotel-ads performance statistics.'))
    # The following argument(s) should be provided to run the example.
#    parser.add_argument('-c', '--customer_id', type=str,
#                        required=True, help='The Google Ads customer ID.')
#    args = parser.parse_args()

    df = main(google_ads_client, '_account_id', _DEFAULT_PAGE_SIZE)
    df['price_level'] = np.select(
			[ df['price_level'] == 0, df['price_level'] == 1,
			  df['price_level'] == 3, df['price_level'] == 4
			],
			[ 'UNSPECIFIED', 'UNKNOWN',
			 'LOWEST_TIED', 'NOT_LOWEST'
			],
			default = df['price_level']
		       )

    df['device'] = np.select(
			[ df['device'] == 0, df['device'] == 1, df['device'] ==  2,
			  df['device'] == 3, df['device'] == 4, df['device'] == 5,
			 df['device'] == 6
			],
			['UNSPECIFIED','UNKNOWN','MOBILE','TABLET','DESKTOP',
			 'OTHER', 'CONNECTED_TV'
			],
			default = df['device']
                       )

    df['date_set'] = np.select(
                        [ df['date_set'] == 0, df['date_set'] == 1,
                          df['date_set'] == 50, df['date_set'] == 51
                        ],
                        ['UNSPECIFIED','UNKNOWN','DEFAULT_SELECTION','USER_SELECTED'
                        ],
                        default = df['device']
                       )

    df['user_country'] = np.select(
			[ df['user_country'].str.extract(r'Constants/(\d+)') == '2158'
			],
			['Taiwan'
			],
                        default = df['user_country']
                       )

    print(len(df))
