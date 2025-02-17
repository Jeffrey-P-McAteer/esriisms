# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "shapely>=2.0.7",
# ]
# ///

import os
import sys
import urllib.request
import urllib.parse
import random
import json

import shapely.geometry

min_x = -120.0
max_y = 46.0
max_x = -82.0
min_y = 32.0

triangle_radius_deg = 6.0
rp = (random.uniform(min_x, max_x), random.uniform(min_y, max_y))


def gen_rand_points(num_points=3):
  pts = []
  for i in range(0, num_points):
    pts.append(
        (rp[0] + random.uniform(-triangle_radius_deg, triangle_radius_deg),
         rp[1] + random.uniform(-triangle_radius_deg, triangle_radius_deg)))
  return pts


a_random_triangle = shapely.geometry.Polygon(gen_rand_points())

oids_observed = set()


def query_features_under(a_polygon, resultOffset=0, resultRecordCount=4):
  global oids_observed
  #json_query_g = json.dumps(shapely.geometry.mapping(a_polygon)) # This isn't effective
  coords = [pt for pt in a_polygon.exterior.coords]
  #print(f'coords = {coords}')
  json_query_g = {'spatialReference': {'wkid': 4326}, 'rings': [coords]}
  #print(f'json_query_g={json_query_g}')
  data = urllib.parse.urlencode({
      'geometry': json.dumps(json_query_g),
      'geometryType': 'esriGeometryPolygon',
      'outFields': '*',
      'returnGeometry': True,
      'resultOffset': resultOffset,
      'resultRecordCount': resultRecordCount,
      'f': 'pjson',
  }).encode()
  req = urllib.request.Request(
      'https://sampleserver6.arcgisonline.com/arcgis/rest/services/USA/MapServer/0/query',
      data=data)
  resp = urllib.request.urlopen(req)
  resp_txt = resp.read()
  #print(f'resp_txt={resp_txt.decode("utf-8")}')
  resp_json = json.loads(resp_txt)
  #print(f'resp_json={resp_json}')
  new_features = [
      f for f in resp_json['features']
      if not (f['attributes']['objectid'] in oids_observed)
  ]
  # Record seen OIDS to disallow them in the future
  for f in resp_json['features']:
    oids_observed.add(f['attributes']['objectid'])

  return len(new_features)


def sum_all_feature_pages_under(a_polygon, page_size=None):
  global oids_observed
  if page_size is None:
    page_size = random.randint(2, 8)
  oids_observed = set()  # Clear this
  total_fc = 0
  result_offset = 0
  allowed_zero_results = 26
  while allowed_zero_results > 0:
    num_features = query_features_under(a_polygon,
                                        resultOffset=result_offset,
                                        resultRecordCount=page_size)
    result_offset += num_features
    total_fc += num_features
    if num_features < 1:
      allowed_zero_results -= 1
  if not (total_fc == len(oids_observed)):
    print(
        f'WARNING: total_fc={total_fc} and len(oids_observed)={len(oids_observed)} ({oids_observed})'
    )
  paginated_oids_observed = list(oids_observed)
  oids_observed = set()  # Clear this
  single_query_total = query_features_under(a_polygon,
                                            resultOffset=0,
                                            resultRecordCount=99999)
  if not (single_query_total == total_fc):
    print(
        f'WARNING: total_fc={total_fc} but single_query_total={single_query_total};'
    )
    print(f'len(oids_observed)={len(oids_observed)} ({oids_observed})')
    print(
        f'len(paginated_oids_observed)={len(paginated_oids_observed)} ({paginated_oids_observed})'
    )

  return total_fc


def test_if_polygon_stable(p):
  global oids_observed
  oids_observed = set()
  #previous_fc = query_features_under(p,
  #                                   resultOffset=0,
  #                                   resultRecordCount=99999)
  previous_fc = sum_all_feature_pages_under(p)
  for i in range(0, 6):
    print('.', end='', flush=True)
    oids_observed = set()
    dis_fc = sum_all_feature_pages_under(p)
    if dis_fc != previous_fc:
      print()
      print(
          f'On query number {i} to server we first saw {previous_fc} features and THEN saw {dis_fc} features.'
      )
      print(f'This polygon broke the stability: {p}')
      return False

    previous_fc = dis_fc
    # print(f'query_features_under(p)={query_features_under(p)}')
  print()
  print(
      f'The following polygon is stable, returns {previous_fc} features 6x times: {p}'
  )
  return True


while query_features_under(a_random_triangle) < 1:
  a_random_triangle = shapely.geometry.Polygon(gen_rand_points(3))

for x in range(0, 20):
  num_pts_to_gen = 3
  while test_if_polygon_stable(a_random_triangle):
    print()
    print()
    a_random_triangle = shapely.geometry.Polygon(
        gen_rand_points(num_pts_to_gen))
    while query_features_under(a_random_triangle) < 1:
      a_random_triangle = shapely.geometry.Polygon(
          gen_rand_points(num_pts_to_gen))
    num_pts_to_gen += 1

print(f'Done!')
