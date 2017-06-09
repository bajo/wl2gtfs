#!/bin/env python

import csv
import urllib2
import os
import json
import io

stops_url = "wienerlinien-ogd-haltestellen.csv"
platforms_url = "wienerlinien-ogd-steige.csv"
lines_url = "wienerlinien-ogd-linien.csv"

def download_file(file_url):
    base_url = "http://data.wien.gv.at/csv/"
    url = base_url+file_url
    u = urllib2.urlopen(url)
    f = open(file_url, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (url, file_size)

    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8) * (len(status) + 1)
        print status,

    f.close()

def check_local_file(file_name):
    if os.path.isfile(file_name):
        print('File {} exists. Skip download.'.format(file_name))
        return
    download_file(file_name)

def read_csv_file(file_name):
    with open(file_name, 'rb') as f:
        reader = csv.reader(f, skipinitialspace=True, delimiter=';')
        header = next(reader)
        data = [dict(zip(header, row)) for row in reader]
    return data

def main():
    data = []
    missing_data = []

    for i in [stops_url, platforms_url, lines_url]:
        check_local_file(i)

    stops = read_csv_file(stops_url)            # Haltestellen
    platforms = read_csv_file(platforms_url)    # Steige
    lines = read_csv_file(lines_url)            # Linien

    for s in stops:
        element = {'stop_id': s['HALTESTELLEN_ID'],
                   'type': s['TYP'],
                   'DIVA': s['DIVA'],
                   'name': s['NAME'],
                   'municipality': s['GEMEINDE'],
                   'municipality_id': s['GEMEINDE_ID'],
                   'wgs84_latitude': s['WGS84_LAT'],
                   'wgs84_longitude': s['WGS84_LON'],
                   'timestamp': '',             # should we fill this with current timestamp?
                   'platforms': [],
                   'lines': []}

        same_stations = filter(lambda plat: plat['FK_HALTESTELLEN_ID'] == s['HALTESTELLEN_ID'], platforms)
        for i in same_stations:
            same_lines = filter(lambda line: line['LINIEN_ID'] == i['FK_LINIEN_ID'], lines)
            for j in same_lines:
                element['lines'].append(j['BEZEICHNUNG'])
                platform = {'line': j['BEZEICHNUNG'],
                            'realtime': j['ECHTZEIT'],
                            'type': j['VERKEHRSMITTEL'],
                            'RBL_number': i['RBL_NUMMER'],
                            'region': i['BEREICH'],
                            'direction': i['RICHTUNG'],
                            'order': i['REIHENFOLGE'],
                            'gate': i['STEIG'],
                            'gate_wgs84_latitude': i['STEIG_WGS84_LAT'],
                            'gate_wgs84_longitude': i['STEIG_WGS84_LON']}
                element['platforms'].append(platform)
        element['lines'] = list(set(element['lines']))

        if not (element['lines'] and element['platforms']):
            missing_data.append(element)
            continue
        data.append(element)

    print('done filling the data structure')
    print('elements converted successfully: {}'.format(len(data)))
    print('elements without platforms and lines {}'.format((len(missing_data))))
    print(data[777])

    with open('wienerlinien.json', 'w') as json_file:
        json.dump(data, json_file, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))
    print('data written to json file')

if __name__ == '__main__':
    main()