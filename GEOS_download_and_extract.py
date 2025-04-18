import requests
import datetime as dt
import numpy as np
import time
import os
import netCDF4 as nc
import glob
from urllib.request import urlopen

savepath = '/home/sprixin/source/GEOS_CF_AOD/src/'
gmao_geos_url = 'https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/forecast/'
ncoutpath = '/home/sprixin/source/GEOS_CF_AOD/extract/'
force_extract = True
del_more = True


# 强行插值是否开启、开启后如果文件缺少或者文件下载有错依旧继续插值，跳过错误文件
# https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/forecast/Y2024/M04/D08/H12/GEOS-CF.v01.fcst.xgc_tavg_1hr_g1440x721_x1.20240408_12z%2B20240408_1230z.nc4

def download_nc_data(url, save_path):
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


def downfile_cont(url, savefilepath):
    file_size = int(urlopen(url).info().get('Content-Length', -1))
    if os.path.exists(savefilepath):
        first_byte = os.path.getsize(savefilepath)
    else:
        first_byte = 0
    header = {"Range": "bytes=%s-%s" % (first_byte, file_size)}
    req = requests.get(url, headers=header, stream=True, timeout=10)
    with(open(savefilepath, 'ab')) as f:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def areadata(lon, lat, data, axes=None):
    if axes is None:
        axes = [70, 140, 5, 60]
    lon_index_w = np.where(np.array(lon) >= axes[0])[0][0]
    lon_index_e = np.where(np.array(lon) <= axes[1])[-1][-1] + 1
    lat_index_s = np.where(np.array(lat) <= axes[2])[-1][-1]
    lat_index_n = np.where(np.array(lat) >= axes[3])[0][0] + 1
    zh_lon = lon[lon_index_w: lon_index_e]
    zh_lat = lat[lat_index_s: lat_index_n]
    if len(np.shape(data)) == 2:
        zh_data = data[lat_index_s: lat_index_n, lon_index_w: lon_index_e]
    else:
        lat_index = np.shape(data).index(len(lat))
        if lat_index == 2:
            zh_data = data[:, lat_index_s: lat_index_n, lon_index_w: lon_index_e]
        elif lat_index == 3:
            zh_data = data[:, :, lat_index_s: lat_index_n, lon_index_w: lon_index_e]

    return zh_lon, zh_lat, zh_data


localtime = dt.datetime.now()
utctime = localtime - dt.timedelta(hours = 8)

if utctime.hour < 12:
    utcdowntime = utctime - dt.timedelta(days=1)
else:
    utcdowntime = utctime

##
#utcdowntime = dt.datetime(2025,2,18)
##

utcdownpathstr = utcdowntime.strftime('Y%Y/M%m/D%d/')
utcdownfilestartstr = utcdowntime.strftime('%Y%m%d')
downpathstr = gmao_geos_url + utcdownpathstr + 'H12/'
savepath = savepath + utcdownfilestartstr + '/'
if not (os.path.exists(savepath)):
    os.mkdir(savepath)

forcstartstr = dt.datetime.strptime(utcdownfilestartstr, '%Y%m%d') + dt.timedelta(hours=11) + dt.timedelta(minutes=30)
forcendstr = [forcstartstr + dt.timedelta(hours=i) for i in range(1, 121)]
downfilestr = ['GEOS-CF.v01.fcst.xgc_tavg_1hr_g1440x721_x1.' + forcstartstr.strftime('%Y%m%d') + '_12z%2B' + forcendstr[
    i].strftime('%Y%m%d_%H%Mz.nc4') for i in range(len(forcendstr))]
downfilename = [
    'GEOS-CF.v01.fcst.xgc_tavg_1hr_g1440x721_x1.' + forcstartstr.strftime('%Y%m%d') + '_12z+' + forcendstr[i].strftime(
        '%Y%m%d_%H%Mz.nc4') for i in range(len(forcendstr))]

downurlstr = [downpathstr + downfilestr[i] for i in range(len(downfilestr))]
savefilepath = [savepath + downfilename[i] for i in range(len(downfilename))]

# 重访爬取
tstart = dt.datetime.now()
print('START DOWNLOADING GEOS-CF.v01.fcst.xgc in ' + tstart.strftime('%Y%m%d %H:%M:%S'))
downindex = range(len(downfilename))

while len(downindex) > 0:
    k = 0
    down_error = np.array([])
    for num in downindex:
        # 超时
        timedown = (dt.datetime.now() - tstart).total_seconds()
        if timedown > 3600 * 24:
            print('ERROR!!! Download Time Out, Plz Chk')
            break
        if os.path.exists(savefilepath[int(num)]):
            try:
                nctest = nc.Dataset(savefilepath[int(num)])
                nctest.close()
                continue
            except Exception as E:
                pass
        k += 1
        print('DOWNLOADING...GEOS-CF_xgc', downfilename[int(num)][-31:-5], '  [%3i/%i]'
              % ((num + 1) if len(downindex) == 120 else k, len(downindex)))
        try:
            # urllib.request.urlretrieve(downurlstr[int(num)], savefilepath[int(num)])
            # download_nc_data(downurlstr[int(num)], savefilepath[int(num)])
            downfile_cont(downurlstr[int(num)], savefilepath[int(num)])
            nctest = nc.Dataset(savefilepath[int(num)])
            print('    SUCCESS...  ERROR Files [%i]' % len(down_error))
            nctest.close()
        except Exception as E:
            if 'Unknown file format' in str(E):
                os.remove(savefilepath[int(num)])
            down_error = np.append(down_error, num)
            print('    FAILED!...  ERROR Files [%i]' % len(down_error), E)
        time.sleep(5)
    downindex = down_error if timedown < 3600 * 24 else []
    if len(downindex) > 0:
        print('Re-Connecting .... File --> [%i]' % len(downindex))
    else:
        print('GEOS DATA Download Complete !!!')

tend = dt.datetime.now()
tused = (tend - tstart).total_seconds()
if tused < 3600:
    print('Finished Downloading GEOS-CF: %.2f Mins' % (tused / 60))
else:
    print('Finished Downloading GEOS-CF: %.2f Hours' % (tused / 3600))

# extract
ncfilepath = savepath
ncloadname = glob.glob(ncfilepath + '*.nc4')
ncloadname = sorted(ncloadname)
ncfilesize = np.zeros(120) * np.nan
if len(ncloadname) < 120:
    print('Warning!!!: file number is less than 120 --> (%i/120)' % len(ncloadname))
elif len(ncloadname) == 120:
    print('NC4 files all ready... start extract')
for num in range(len(ncloadname)):
    ncfilesize[num] = os.path.getsize(ncloadname[num])
    if ncfilesize[num] < 1e+8: print('Warning!!!: file size chk error --> ' + ncloadname[num][-31:])

# 头nc数据导入、基本数据特征导入、元素长度定义。。。
geosdata = nc.Dataset(ncloadname[0])
lon = np.array(geosdata.variables['lon'][:])
lat = np.array(geosdata.variables['lat'][:])
t = geosdata.variables['time']
timestr = dt.datetime.strptime(str(t.begin_date) + str(t.begin_time), '%Y%m%d%H%M%S')
timedate = dt.datetime.strftime(timestr, '%Y%m%d%H%M')

aod550_dust = np.array(geosdata.variables['AOD550_DUST'][0, :, :])
aod550_cloud = np.array(geosdata.variables['AOD550_CLOUD'][0, :, :])
aod550_bc = np.array(geosdata.variables['AOD550_BC'][0, :, :])

lon_zh, lat_zh, aod550_dust_zh = areadata(lon, lat, aod550_dust)
lon_zh, lat_zh, aod550_cloud_zh = areadata(lon, lat, aod550_cloud)
lon_zh, lat_zh, aod550_bc_zh = areadata(lon, lat, aod550_bc)

aod550_dust_geos = np.ones((len(ncloadname), len(lat_zh), len(lon_zh))) * np.nan
aod550_cloud_geos = np.ones((len(ncloadname), len(lat_zh), len(lon_zh))) * np.nan
aod550_bc_geos = np.ones((len(ncloadname), len(lat_zh), len(lon_zh))) * np.nan
time_geos = []

aod550_dust_geos[0, :, :], aod550_cloud_geos[0, :, :], aod550_bc_geos[0, :, :] = (
    aod550_dust_zh, aod550_cloud_zh, aod550_bc_zh)
time_geos.append(int(timedate))

readfailed = np.array([])

if (len(ncloadname) < 120) & (not force_extract):
    print('ERROR!!! NC Files in need')
elif (len(ncloadname) < 120) & force_extract:
    print('Attention !!! FORCE_extract USED...Missing data ---> (%i/120)' % len(ncloadname))
else:
    print('Start Section and Region Area.... Files Number: [%i]' % len(ncloadname))

for num in range(1, len(ncloadname)):
    if (len(ncloadname) < 120) & (not force_extract): break
    if ncfilesize[num] < 1e+8:
        readfailed = np.append(readfailed, num)
        continue
    try:
        geosdata = nc.Dataset(ncloadname[num])
        t = geosdata.variables['time']
        timestr = dt.datetime.strptime(t.units[-19:], '%Y-%m-%d %H:%M:%S')
        timedate = dt.datetime.strftime(timestr, '%Y%m%d%H%M')
        aod550_dust = np.array(geosdata.variables['AOD550_DUST'][0, :, :])
        aod550_cloud = np.array(geosdata.variables['AOD550_CLOUD'][0, :, :])
        aod550_bc = np.array(geosdata.variables['AOD550_BC'][0, :, :])
        lon_zh, lat_zh, aod550_dust_zh = areadata(lon, lat, aod550_dust)
        lon_zh, lat_zh, aod550_cloud_zh = areadata(lon, lat, aod550_cloud)
        lon_zh, lat_zh, aod550_bc_zh = areadata(lon, lat, aod550_bc)
        aod550_dust_geos[num, :, :], aod550_cloud_geos[num, :, :], aod550_bc_geos[num, :, :] = (
            aod550_dust_zh, aod550_cloud_zh, aod550_bc_zh)
        time_geos.append(int(timedate))
        print('Running....: [%3i/%3i]' % (num + 1, len(ncloadname)), end='\r')
    except Exception as E:
        print(num, E)
        readfailed = np.append(readfailed, num)

if len(readfailed) == 0:
    print('\nSUCCESS!!!...GEOS DATA All Exits')
else:
    print('\nATTENTION!!! GEOS DATA Break/Lost --> Number: %i' % len(readfailed))
    print(f'File List >> {[ncloadname[int(i)] for i in readfailed]}')

# nc file output
ncoutfilename = 'GEOS-CF.fcst.AOD.' + str(time_geos[0]) + '.nc4'
ncoutfilepath = ncoutpath + ncoutfilename

timegeos = []
for num in range(len(time_geos)):
    timegeos.append(dt.timedelta.total_seconds(
        dt.datetime.strptime(str(time_geos[num]), '%Y%m%d%H%M') -
        dt.datetime(1970, 1, 1)) / 3600)
timegeos = np.array(timegeos, dtype='float64')

if os.path.exists(ncoutfilepath):
    os.remove(ncoutfilepath)
geos_nc = nc.Dataset(ncoutfilepath, 'w', format='NETCDF4')
try:
    geos_nc.createDimension('latitude', len(lat_zh))
    geos_nc.createDimension('longitude', len(lon_zh))
    geos_nc.createDimension('time', len(time_geos))

    geos_nc.createVariable("latitude", 'f', 'latitude')
    geos_nc.createVariable("longitude", 'f', 'longitude')
    nctime = geos_nc.createVariable("time", 'f', 'time')
    nctime.units = 'hours since 1970-01-01 00:00:00'
    nctime.calendar = 'gregorian'
    geos_nc.createVariable("AOD550_DUST", 'f', ("time", "latitude", "longitude"))
    geos_nc.createVariable("AOD550_CLOUD", 'f', ("time", "latitude", "longitude"))
    geos_nc.createVariable("AOD550_BC", 'f', ("time", "latitude", "longitude"))

    geos_nc.variables['latitude'][:] = lat_zh
    geos_nc.variables['longitude'][:] = lon_zh
    geos_nc.variables['time'][:] = timegeos
    geos_nc.variables['AOD550_DUST'][:, :, :] = aod550_dust_geos[:, :, :]
    geos_nc.variables['AOD550_CLOUD'][:, :, :] = aod550_cloud_geos[:, :, :]
    geos_nc.variables['AOD550_BC'][:, :, :] = aod550_bc_geos[:, :, :]
    geos_nc.close()
except Exception as E:
    print(E)
    geos_nc.close()
    
if del_more:
    if utcdowntime.day == 1:
        del_day = (utcdowntime - (utcdowntime - dt.timedelta(days = 10)).replace(day = 1)).days
        del_list = [savepath + ((utcdowntime - dt.timedelta(days = 10)).replace(day = 1) + dt.timedelta(days = t)).strftime('%Y%m%d') for t in range(del_day)]
        for del_file in del_list:
            if os.path.exists(del_file):os.removedirs(del_file)
