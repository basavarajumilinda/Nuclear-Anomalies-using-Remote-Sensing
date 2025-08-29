// ZNPP — FIRMS NRT (MODIS)
var pt  = ee.Geometry.Point([34.5851, 47.5067]);  // [lon, lat]
var aoi = pt.buffer(2000);                       // 2 km
var start = '2024-07-02';
var end   = '2024-07-05';  

Map.centerObject(aoi, 11);
Map.addLayer(aoi, {color:'white'}, 'AOI 20 km');

// Filter FIRMS NRT
var firms = ee.ImageCollection('FIRMS')
  .filterBounds(aoi)
  .filterDate(start, end);

// Confidence threshold 
var confMin = 60;

// Boolean mask of fire pixels for each day
var fmCol = firms.map(function(img){
  var m = img.select('confidence').gte(confMin).selfMask().rename('fire');
  return m.copyProperties(img, ['system:time_start']);
});

// Daily counts 
var daily = fmCol.map(function(fm){
  var dict = fm.reduceRegion({
    reducer: ee.Reducer.count(),
    geometry: aoi, scale: 1000, maxPixels: 1e9
  });
  var n = ee.Number(ee.Dictionary(dict).get('fire', 0));
  return ee.Feature(null, {
    date: ee.Date(fm.get('system:time_start')).format('YYYY-MM-dd'),
    px: n
  });
});

print('FIRMS daily fire px (NRT, MODIS)', daily);
var chart = ui.Chart.feature.byFeature(daily, 'date', 'px')
  .setChartType('ColumnChart')
  .setOptions({title:'FIRMS (MODIS) fire px/day', legend:'none'});
print(chart);

// Visualize 
var any = fmCol.max().clip(aoi);
Map.addLayer(any.reproject('EPSG:4326', null, 500),
  {min:0, max:1, palette:['#ff3b3b'], opacity:0.9},
  'FIRMS (MODIS) any in 2–4 Jul', true);

// Just 3 July 2024
var fm_0703 = fmCol
  .filterDate('2024-07-03', '2024-07-04')
  .max().clip(aoi);
Map.addLayer(fm_0703.reproject('EPSG:4326', null, 500),
  {min:0, max:1, palette:['#ff9900'], opacity:0.95},
  'FIRMS (MODIS) — 2024-07-03', true);
