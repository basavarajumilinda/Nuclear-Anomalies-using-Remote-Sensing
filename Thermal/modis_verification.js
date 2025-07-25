
function makeRectangle(point, xRadius, yRadius, proj) {
  // Define a Point object.
  var roi = ee.Geometry.Point(point[0], point[1]);
  var roiBuffer = roi.buffer({'distance': 6000}).bounds();
  return roiBuffer;
}

// Define inputs for making a rectangle from center point.
var point = [34.585, 47.498];  // lat, lon


// Make a rectangle from center point.
var roi = makeRectangle(point);  

var dataset = ee.ImageCollection('MODIS/061/MOD14A1')
                  .filter(ee.Filter.date('2022-08-24', '2022-08-25'));
var dataset = dataset.first()
var fireMaskVis = {
  min: 0.0,
  max: 6000.0,
  bands: ['MaxFRP', 'FireMask', 'FireMask'],
};
var s2 = ee.Image('COPERNICUS/S2/20220824T083559_20220824T083607_T36TXT')
var rgbVis = {
  min: 0.0,
  max: 4000,
  bands: ['B4', 'B3', 'B2'],
};
var dataset = dataset
    .unitScale(-2000, 10000)
    .reproject('EPSG:4326', null, 500);
    
Map.setCenter(6.746, 46.529, 2);
Map.addLayer(s2, rgbVis, 'S2')
Map.addLayer(dataset, fireMaskVis, 'Fire Mask');





