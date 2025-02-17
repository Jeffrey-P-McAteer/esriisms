# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "shapely>=2.0.7",
#   "pyproj>=2.3.0"
# ]
# ///

import os
import sys
import urllib.request
import urllib.parse
import random
import json
import traceback
import pyproj
import shapely.geometry

server_urls = [ random.choice([
  # This one is an older server (10.91) which does not give stable paginated results
  'https://sampleserver6.arcgisonline.com/arcgis/rest/services/USA/MapServer/0/query',
  # This is a very recent version (11.2) which gives stable paginated results
  'https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Major_Cities_/FeatureServer/0/query',
  # New version (11.3), still unstable pages!
  'https://gis.blm.gov/arcgis/rest/services/recreation/BLM_Natl_Recreation_Sites_Facilities/MapServer/1/query',
  # New version (11.1), no unstable pages seen.
  'https://energy.virginia.gov/gis/rest/services/DGMR/VA_Water_Wells/MapServer/0',
]) ]

if len(sys.argv) > 1:
  server_urls = sys.argv[1:]

possible_oid_names = [
  'objectid', 'OBJECTID', 'ObjectID', 'oid', 'OID', 'rowid'
]
server_oid_field_name = 'objectid'

min_x = -120.0
max_y = 46.0
max_x = -82.0
min_y = 32.0

# triangle_radius_deg = 6.0

def area_of_wgs84_in_km2(g):
  geod = pyproj.Geod(ellps='WGS84')
  return abs(geod.geometry_area_perimeter(g)[0])

def gen_rand_points(num_points=3):
  global min_x, max_x, min_y, max_y
  rp = (random.uniform(min_x, max_x), random.uniform(min_y, max_y))
  triangle_radius_deg = abs(max_x - min_x) / 4.0 # Make the triangles approx 1/4 by 1/4 in size, so querying approx 1/16th of data or so.
  pts = []
  for i in range(0, num_points):
    pts.append(
        (rp[0] + random.uniform(-triangle_radius_deg, triangle_radius_deg),
         rp[1] + random.uniform(-triangle_radius_deg, triangle_radius_deg)))
  return pts

def read_server_version(query_url):
  # We just read up 1 directory until we get JSON w/'currentVersion' key
  up_one_url = query_url.split('?')[0]
  for i in range(0, 6):
    up_one_url = '/'.join(up_one_url.split('/')[:-1])
    try:
      req = urllib.request.Request(up_one_url+'?f=pjson')
      resp = urllib.request.urlopen(req)
      resp_txt = resp.read()
      resp_json = json.loads(resp_txt)
      if 'currentVersion' in resp_json:
        return resp_json['currentVersion']
    except:
      traceback.print_exc()
  return -0.0

def read_fc_extent(query_url):
  query_url = query_url.split('?')[0]
  if query_url.endswith('/query'):
    query_url = '/'.join(query_url.split('/')[:-1])
  try:
    req = urllib.request.Request(query_url+'?f=pjson')
    resp = urllib.request.urlopen(req)
    resp_txt = resp.read()
    resp_json = json.loads(resp_txt)
    if 'extent' in resp_json:
      return {
        'xmin': resp_json['extent']['xmin'],
        'xmax': resp_json['extent']['xmax'],
        'ymin': resp_json['extent']['ymin'],
        'ymax': resp_json['extent']['ymax'],
      }
  except:
    traceback.print_exc()
  return {
    'xmin': min_x,
    'xmax': max_x,
    'ymin': min_y,
    'ymax': max_y,
  }

def read_fc_oid_field(query_url):
  global server_oid_field_name
  query_url = query_url.split('?')[0]
  if query_url.endswith('/query'):
    query_url = '/'.join(query_url.split('/')[:-1])
  try:
    req = urllib.request.Request(query_url+'?f=pjson')
    resp = urllib.request.urlopen(req)
    resp_txt = resp.read()
    resp_json = json.loads(resp_txt)
    if 'fields' in resp_json:
      for field in resp_json['fields']:
        if field.get('type', 'UNK').casefold() == 'esriFieldTypeOID'.casefold():
          return field.get('name', field.get('alias', server_oid_field_name))

  except:
    traceback.print_exc()
  return server_oid_field_name

def tf_to_yn(tf):
  if tf:
    return 'Yes'
  else:
    return 'No'

def read_oid(f):
  if 'attributes' in f:
    for possible_oid_name in possible_oid_names:
      if possible_oid_name in f['attributes']:
        return f['attributes'][possible_oid_name]
    raise Exception(f'No ObjectID found in f={f}')
  else:
    for possible_oid_name in possible_oid_names:
      if possible_oid_name in f:
        return f[possible_oid_name]
    raise Exception(f'No ObjectID found in f={f}')

def read_oid_field(f):
  if 'attributes' in f:
    for possible_oid_name in possible_oid_names:
      if possible_oid_name in f['attributes']:
        return possible_oid_name
    raise Exception(f'No ObjectID found in f={f}')
  else:
    for possible_oid_name in possible_oid_names:
      if possible_oid_name in f:
        return possible_oid_name
    raise Exception(f'No ObjectID found in f={f}')


last_feature_page_json = None
def query_feature_page(a_polygon, resultOffset=0, resultRecordCount=4):
  global last_feature_page_json, server_oid_field_name
  if a_polygon is None:
    return []
  if hasattr(a_polygon, 'exterior'):
    coords = [pt for pt in a_polygon.exterior.coords]
  else:
    coords = a_polygon
  json_query_g = {'spatialReference': {'wkid': 4326}, 'rings': [coords]}
  data = urllib.parse.urlencode({
      'geometry': json.dumps(json_query_g),
      'geometryType': 'esriGeometryPolygon',
      'outFields': '*',
      'returnGeometry': True,
      'orderByFields': f'{server_oid_field_name} DESC',
      # 'returnDistinctValues': 'true', # Cannot use in a Geometry query -_-
      'resultOffset': resultOffset,
      'resultRecordCount': resultRecordCount,
      'f': 'pjson',
  }).encode()
  req = urllib.request.Request(
      server_url,
      data=data)
  resp = urllib.request.urlopen(req)
  resp_txt = resp.read()
  resp_json = json.loads(resp_txt)
  last_feature_page_json = resp_json # Save off for any reporting we want to do
  # We return a list of objectids
  if not 'features' in resp_json:
    if not isinstance(resp_txt, str):
      resp_txt = resp_txt.decode('utf-8')
    if not isinstance(data, str):
      data = data.decode('utf-8')
    print(f'WARNING ERROR JSON:\n{resp_txt}\n^^ query data={data}\n')
    return []
  return [ read_oid(f) for f in resp_json['features'] ]

def query_all_feature_pages(a_polygon):
  global last_feature_page_json
  if a_polygon is None:
    return []
  allowed_zero_replies = 6
  result_offset = 0
  offset_and_len = list() # Tuple of (resultOffset, resultRecordCount)
  pages_of_oids = list()
  while allowed_zero_replies > 0:
    result_record_count = random.choice([4,5,6,7,8,9,10])
    feature_page = query_feature_page(
      a_polygon,
      resultOffset=result_offset,
      resultRecordCount=result_record_count # We select a random page size - when joined this should not make a difference if we're reading features 2-at-a-time, 3-at-a-time, etc.
    )
    if len(feature_page) < 1:
      allowed_zero_replies -= 1
      #if 'exceededTransferLimit' in last_feature_page_json:
      #  print(f'NOTE: Zero response! resultOffset={result_offset} resultRecordCount={result_record_count}')
      #  if last_feature_page_json['exceededTransferLimit']:
      #    # features is [], AND exceededTransferLimit is True
      #    # Hmmmm we really want the first feature. How can we get it?
      #    if random.choice([True, False, False]):
      #      allowed_zero_replies += 1
      #    result_offset += 1 # We decide to SKIP the first feature; no idea what this will bring.
    else:
      # got some features, reset allowed_zero_replies!
      allowed_zero_replies = 6

    pages_of_oids.append(feature_page)
    offset_and_len.append(
      (result_offset, result_record_count)
    )

    result_offset += len(feature_page)

  return offset_and_len, pages_of_oids



if __name__ == '__main__':
  for server_url in server_urls:
    # Step 0: Report meta-data
    server_host = urllib.parse.urlparse(server_url).netloc
    print('='*12, f'TEST BEGIN FOR {server_host}', '='*12)
    print(f'Server under test = {server_url}')
    server_version = read_server_version(server_url)
    print(f'Server version = {server_version}')
    fc_extent = read_fc_extent(server_url)
    min_x = fc_extent.get('xmin', min_x)
    max_y = fc_extent.get('ymax', max_y)
    max_x = fc_extent.get('xmax', max_x)
    min_y = fc_extent.get('ymin', min_y)
    print(f'min_x={min_x} max_x={max_x} min_y={min_y} max_y={max_y}')

    server_oid_field_name = read_fc_oid_field(server_url)
    if not server_oid_field_name in possible_oid_names:
      possible_oid_names.insert(0, server_oid_field_name)

    # Step 1: Generate a triangle which, when queried for UP to 500 features returns at least 9 and less than 300.
    g = None
    while True:
      num_features = len(query_feature_page(g, resultOffset=0, resultRecordCount=500))
      if num_features > 9 and num_features < 300:
        break
      g = shapely.geometry.Polygon(gen_rand_points(3))
    print(f'Running test with random Geometry {g}')
    print(f'Test geometry is {int(area_of_wgs84_in_km2(g)):,} km^2 in area')
    print()

    # Step 2: Log the "expected" ordering of OIDS
    expected_oids = query_feature_page(g, resultOffset=0, resultRecordCount=500)
    print(f'expected_oids({len(expected_oids)}) = {expected_oids}')

    # Step 3: Join pages together and do analysis on
    #   - do OIDS repeat?
    #   - Are OIDS omitted?
    offset_and_len, pages_of_oids = query_all_feature_pages(g)
    print(f'=== {len(pages_of_oids)} pages of oids returned ===')
    for i in range(0, min(len(pages_of_oids), len(offset_and_len)) ):
      print(f'  Requested begin at offset {offset_and_len[i][0]: <2}, return the next {offset_and_len[i][1]: <2} items, recived {len(pages_of_oids[i]): <2}: {pages_of_oids[i]}')

    pages_of_oids_unique_oids = set()
    flattened_returned_pages = list()
    for p in pages_of_oids:
      for oid in p:
        pages_of_oids_unique_oids.add(oid)
        flattened_returned_pages.append(oid)

    print(f'pages_of_oids_unique_oids({len(pages_of_oids_unique_oids)}) = {pages_of_oids_unique_oids}')

    print()
    print('Q1: Are there duplicate OIDs?')
    q1_is_true = False
    oid_counts = dict()
    for oid in flattened_returned_pages:
      if not oid in oid_counts:
        oid_counts[oid] = 0
      oid_counts[oid] += 1

    for oid, count in oid_counts.items():
      if count > 1:
        print(f'  Observation: {oid} was returned {count} times!')
        q1_is_true = True
    print(f'Q1 is {tf_to_yn(q1_is_true)} for {server_host} running version {server_version}')
    print()

    print('Q2: Are there expected OIDs which were NOT returnd by the paginated query?')
    q2_is_true = False
    for e_oid in expected_oids:
      if not e_oid in pages_of_oids_unique_oids:
        print(f'  Observation: {e_oid} was NOT returned in the pages!')
        q2_is_true = False
    print(f'Q2 is {tf_to_yn(q2_is_true)} for {server_host} running version {server_version}')
    print()

    print('Q3: Is the ordering different from the one big query to the combination of smaller queries?')
    q3_is_true = False
    for i in range(0, min(len(expected_oids), len(flattened_returned_pages))):
      if expected_oids[i] != flattened_returned_pages[i]:
        print(f'  Expected OID {expected_oids[i]} at position {i} but flattened_returned_pages[{i}] = {flattened_returned_pages[i]}')
        q3_is_true = True
    print(f'Q3 is {tf_to_yn(q3_is_true)} for {server_host} running version {server_version}')
    print()
    print('='*12, f'TEST END FOR {server_host}', '='*12)
    print()
