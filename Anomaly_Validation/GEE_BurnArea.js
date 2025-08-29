

// (1) Tight plant polygon 
var plant = ee.Geometry.Rectangle([34.5765, 47.4945, 34.6010, 47.5095]).buffer(10);

// (2) Dates 
var start = '2024-07-03';
var end   = '2024-07-04';

Map.centerObject(plant, 14);
Map.addLayer(plant, {color:'yellow'}, 'Plant boundary');


function centersWithTime(colId, scale, label) {
  
  var col0 = ee.ImageCollection(colId).filterDate(start, end);

  // If empty, return empty FeatureCollection
  var count = col0.size();
  var emptyFC = ee.FeatureCollection([]);
  var fc = ee.Algorithms.If(count.eq(0), emptyFC, col0.map(function(img){
    // Boolean FireMask (>=7) for this image
    var fm = img.select('FireMask').gte(7).selfMask().rename('fire');

    // Use THIS image's projection so reduceToVectors has a CRS
    var proj = img.select('FireMask').projection();

    // Vectorize pixel centers for this image, then keep those inside the plant
    var v = fm.reduceToVectors({
      geometry: plant.buffer(10),
      geometryType: 'centroid',
      scale: scale,                 // 375 for VIIRS, 1000 for MODIS
      crs: proj.crs(),
      maxPixels: 1e9
    }).map(function(feat){
      return feat.set({
        sensor: label,
        time: ee.Date(img.get('system:time_start')).format('YYYY-MM-dd HH:mm')
      });
    });

    // Only keep vectors whose centers fall inside the plant
    return v.filterBounds(plant.buffer(10));
  }).flatten());

  fc = ee.FeatureCollection(fc);   

  print(label + ' fire-pixel centers INSIDE plant:', fc.size());
  Map.addLayer(fc, {color: label.indexOf('VIIRS')>-1 ? 'cyan' : 'orange'},
               label + ' centers in-plant', true);
  return fc;
}

var viirsCenters = centersWithTime('NOAA/VIIRS/001/VNP14A1', 375,  'VIIRS 375m');
var modisCenters = centersWithTime('MODIS/061/MOD14A1',      1000, 'MODIS 1km');

// Optional tidy table
var allCenters = viirsCenters.merge(modisCenters).select(['time','sensor']);
print('AF centers (time & sensor):', allCenters);


function maskS2(img){
  var scl = img.select('SCL');
  return img.updateMask(
    scl.neq(3).and(scl.neq(8)).and(scl.neq(9)).and(scl.neq(10)).and(scl.neq(11))
  );
}
function NBR(img){ return img.normalizedDifference(['B8','B12']).rename('NBR'); }

var pre  = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
  .filterBounds(plant).filterDate('2024-06-15','2024-07-02').map(maskS2).median();
var post = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
  .filterBounds(plant).filterDate('2024-07-03','2024-07-06').map(maskS2).median();

var dNBR   = NBR(pre).subtract(NBR(post)).rename('dNBR');
var preNDVI= pre.normalizedDifference(['B8','B4']).rename('NDVI_pre');
var vegMask= preNDVI.gt(0.30);

// Burn masks
var burnAny = dNBR.gt(0.27).selfMask().clip(plant);
var burnVeg = dNBR.gt(0.27).updateMask(vegMask).selfMask().clip(plant);

// Visualize
Map.addLayer(dNBR.clip(plant), {min:-0.2,max:0.6, palette:['#a6cee3','#e31a1c','#ff7f00']}, 'dNBR (plant)', false);
Map.addLayer(burnAny, {palette:['#ff0000']}, 'Burn mask (>0.27) in plant', false);
Map.addLayer(burnVeg, {palette:['#00ff00']}, 'Burn on PRIOR vegetation (>0.27)', true);

// Areas (hectares)
function areaHa(maskImg){
  return ee.Number(
    maskImg.multiply(ee.Image.pixelArea())
           .reduceRegion({reducer: ee.Reducer.sum(), geometry: plant, scale: 20, maxPixels: 1e9})
           .get('dNBR')
  ).divide(1e4);
}

print('Burn area inside plant (all surfaces, ha):', areaHa(burnAny));
print('Burn area on PRIOR vegetation inside plant (ha):', areaHa(burnVeg));
