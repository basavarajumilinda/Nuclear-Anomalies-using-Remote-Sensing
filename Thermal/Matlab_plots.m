% Load Normal Data (2015–2022) 
LandsatTable = readtable("D:/Dissertation-2542000/RP3/Thermal/Notebooks/stats_normal_2015_merged.csv");
%LandsatTable = readtable("D:/Dissertation-2542000/RP3/Thermal/Notebooks/Fordo_stats_normal_2015_merged.csv");
LandsatTable.LandsatAcquisitionDate = datetime(LandsatTable.LandsatAcquisitionDate, 'InputFormat', 'dd/MM/yyyy');
LandsatTable = sortrows(LandsatTable, 'LandsatAcquisitionDate');

x1 = LandsatTable.LandsatAcquisitionDate;
y1 = LandsatTable.MaxTemp;
y1mean = LandsatTable.MeanTemp;

% Load Downscaled GEE Data (2023–2025) 
DownscaleTable = readtable("D:/Dissertation-2542000/RP3/Thermal/Notebooks/stats_downscale_2023_merged.csv");
%DownscaleTable = readtable("D:/Dissertation-2542000/RP3/Thermal/Notebooks/Fordo_stats_downscale_2023_merged.csv");
DownscaleTable.Sentinel2AcquisitionDate = datetime(DownscaleTable.Sentinel2AcquisitionDate, 'InputFormat', 'dd/MM/yyyy');
DownscaleTable = sortrows(DownscaleTable, 'Sentinel2AcquisitionDate');

x2 = DownscaleTable.Sentinel2AcquisitionDate;
y2 = DownscaleTable.MaxTemp;

%  Load Constellr Data 
Constellr = readtable("D:/Dissertation-2542000/RP3/Thermal/Notebooks/zaporizhzhia_LST_summary.csv");
%Constellr = readtable("D:/Dissertation-2542000/RP3/Thermal/Notebooks/fordo_LST_summary.csv");
Constellr.Date = datetime(Constellr.("DateFolder"), 'InputFormat', 'dd-MM-yyyy');
Constellr.MaxTemp = Constellr.("MaxTemp");
Constellr.MeanTemp = Constellr.("MeanTemp");
Constellr.DeltaT = Constellr.MaxTemp - Constellr.MeanTemp;

disp(class(y1))       % Displays the class of y1
disp(class(y1mean))   % Displays the class of y1mean

% Interpolation for Seasonality 

if iscell(y1)
    fprintf("y1 is a cell array. Converting to double...\n");
    y1 = cellfun(@str2double, y1);
else
    fprintf("y1 is already numeric: %s\n", class(y1));
end

if iscell(y1mean)
    fprintf("y1 is a cell array. Converting to double...\n");
    y1mean = cellfun(@str2double, y1mean);
else
    fprintf("y1 is already numeric: %s\n", class(y1));
end

y1a = y1;
x1a = x1;

[datenums_unique, idx] = unique(datenum(x1a));
x1a = x1a(idx);
y1a = y1a(idx);
datenums = datenums_unique;
xx = linspace(datenums(1), datenums(end), 200);
vq2 = interp1(datenums, y1a', xx, 'linear');
t_interp = datetime(xx, 'ConvertFrom', 'datenum');

disp(class(y1))       % Displays the class of y1
disp(class(y1mean))   % Displays the class of y1mean

%  Seasonal Sine Wave 
t = 0:0.01:3000;
A = 20; f = 1/365;
y_season = A * cos(2 * pi * f * t) + 36;

% Plot Max Temperature Trend 
figure;
plot(x1a, y1a, 'ok', 'MarkerSize', 3, 'DisplayName', 'Landsat 8 Max Temp'); hold on;
plot(x2, y2, 'or', 'MarkerSize', 3, 'DisplayName', 'Downscaled Max Temp');
plot(Constellr.Date, Constellr.MaxTemp, 'ob', 'MarkerFaceColor', 'b', 'MarkerSize', 4, 'DisplayName', 'Constellr Max Temp');
plot(t_interp, vq2, 'k', 'DisplayName', 'Interpolated Landsat 8');
plot(t, y_season, '--k', 'DisplayName', 'Seasonality');

ylim([5 75]);
legend('Location', 'northwest');
title('Maximum Recorded Temperature (T \circC)');
ylabel('T \circC');
xlabel('Year');
grid on;

%  Plot Max - Mean Temperature Differences (ΔT)
figure;
vals = y1 - y1mean;
raw_y2 = DownscaleTable.MaxTemp - DownscaleTable.MeanTemp;
Constellr_DeltaT = Constellr.DeltaT;

plot(x1, vals, 'ok', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Landsat 8'); hold on;
plot(x2, raw_y2, 'or', 'MarkerFaceColor','r', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Downscaled');
% plot(Constellr.Date, Constellr_DeltaT, 'x', 'Color', [0.2 0.2 0.2], 'MarkerSize', 5, 'DisplayName', 'Max - Mean: Constellr');
plot(Constellr.Date, Constellr.DeltaT, 'ob', 'MarkerFaceColor', 'b', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Constellr');


% Lognormal distribution fit (2015–2022 baseline) 
meanvals = mean(vals);
stdev = std(vals);
pd = fitdist(vals, 'Lognormal');
perc = icdf(pd, 0.99);
disp(['99th Percentile Threshold (ΔT): ', num2str(perc)])
fprintf('99th Percentile (ΔT): %.4f°C\n', perc)

% Annotate Thresholds 
yline(meanvals + 2 * stdev, '--k', '2\sigma Threshold');
yline(perc, '--r', '99th Percentile');

max_val = max([vals(:); raw_y2(:); Constellr_DeltaT(:); perc]);
ylim([0, ceil(max_val) + 2]);

title('Difference in Max and Mean Temperature (\DeltaT \circC)');
ylabel('\DeltaT \circC');
xlabel('Year');
legend('Location', 'northwest');
box on;
grid on;

% Save processed data (optional) 
LandsatTable.diff_from_mean = vals;
writetable(LandsatTable, 'landsattabledata_updated.xlsx');
DownscaleTable.diff_from_mean = raw_y2;
writetable(DownscaleTable, 'downscaletabledata_updated.xlsx');
writetable(Constellr, 'constellrtabledata_updated.xlsx');