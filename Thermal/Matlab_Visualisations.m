% Updated MATLAB Code for Temperature Time Series and Anomaly Detection
% Using: stats8_normal_2015.xlsx and stats8_downscale.csv

% === Load Normal Data (2015–2022) ===
LandsatTable = readtable("D:/Dissertation-2542000/RP3/stats8_normal_2015.xlsx");
LandsatTable.Properties.VariableNames
LandsatTable.Landsat8AcquisitionDate = datetime(LandsatTable.Landsat8AcquisitionDate, 'InputFormat', 'dd/MM/yyyy');
LandsatTable = sortrows(LandsatTable, 'Landsat8AcquisitionDate');
LandsatTable.Properties.VariableNames

x1 = LandsatTable.Landsat8AcquisitionDate;
y1 = LandsatTable.MaxTemp;
y1mean = LandsatTable.MeanTemp;

whos y1
% === Load Downscaled Data (2023–2025) ===
DownscaleTable = readtable("D:/Dissertation-2542000/RP3/stats8_downscale.csv");
DownscaleTable.Sentinel2AcquisitionDate = datetime(DownscaleTable.Sentinel2AcquisitionDate, 'InputFormat', 'dd/MM/yyyy');
DownscaleTable = sortrows(DownscaleTable, 'Sentinel2AcquisitionDate');

x2 = DownscaleTable.Sentinel2AcquisitionDate;
y2 = DownscaleTable.MaxTemp;

% === Interpolation for Seasonality ===

y1 = cellfun(@str2double, y1);  % Convert cell → double
y1mean = cellfun(@str2double, y1mean);
y1a = y1;  % Now this is safe for interpolation
x1a = x1;

[datenums_unique, idx] = unique(datenum(x1a));  % Keep first occurrence
x1a = x1a(idx);
y1a = y1a(idx);

whos x1a
whos y1a

datenums = datenums_unique
xx = linspace(datenums(1), datenums(end), 200);
vq2 = interp1(datenums, y1a', xx, 'linear');
t_interp = datetime(xx, 'ConvertFrom', 'datenum');

% === Seasonal Sine Wave ===
t = 0:0.01:3000;
A = 20; f = 1/365;
y_season = A * cos(2 * pi * f * t) + 36;

% === Plot Max Temperature Trend ===
figure;
plot(x1a, y1a, 'or', 'MarkerSize', 3, 'DisplayName', 'Landsat 8 Max Temp'); hold on;
plot(x2, y2, 'ok', 'MarkerSize', 3, 'DisplayName', 'Downscaled Max Temp');
plot(t_interp, vq2, 'k', 'DisplayName', 'Interpolated Landsat 8');
plot(t, y_season, '--k', 'DisplayName', 'Seasonality');

ylim([5 75]);
legend;
title('Maximum Recorded Temperature (T \circC)');
ylabel('T \circC');
xlabel('Year');
grid on;

% === Plot Max - Mean Temperature Differences ===
figure;
vals = y1 - y1mean;
raw_y2 = DownscaleTable.MaxTemp - DownscaleTable.MeanTemp;
plot(x1, vals, 'or', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Landsat 8'); hold on;
plot(x2, raw_y2, 'ok', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Downscaled');

meanvals = mean(vals);
stdev = std(vals);

% Lognormal distribution fit
pd = fitdist(vals, 'Lognormal');
perc = icdf(pd, 0.99);
% perc=  42.00  ;                      

% Annotate thresholds
yline(meanvals + 2 * stdev, '--', '2\sigma Threshold');
yline(perc, '--r', '99th Percentile');

max_val = max([vals(:); raw_y2(:); perc]);
ylim([0, ceil(max_val) + 2]);  % Add small buffer

title('Difference in Max and Mean Temperature (\DeltaT \circC)');
ylabel('\DeltaT \circC');
xlabel('Year');
legend;
box on;

% === Save processed data (optional) ===
LandsatTable.diff_from_mean = vals;
writetable(LandsatTable, 'landsattabledata_updated.xlsx');
writetable(DownscaleTable, 'downscaletabledata_updated.xlsx');

%% ===== cauchy distribution fit =====
% figure;

% % Compute Max - Mean differences
% vals = y1 - y1mean;
% raw_y2 = DownscaleTable.MaxTemp - DownscaleTable.MeanTemp;

% % Plot the differences
% plot(x1, vals, 'or', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Landsat 8'); hold on;
% plot(x2, raw_y2, 'ok', 'MarkerSize', 3, 'DisplayName', 'Max - Mean: Downscaled');

% % Mean and std (for reference threshold)
% meanvals = mean(vals);
% stdev = std(vals);

% % === Cauchy Distribution Fit ===
% % Cauchy PDF: f(x; x0, gamma) = 1 / [pi * gamma * (1 + ((x - x0)/gamma)^2)]
% cauchy_pdf = @(x, x0, gamma) (1 ./ (pi * gamma * (1 + ((x - x0)./gamma).^2)));

% % Negative log-likelihood
% negloglik = @(params) -sum(log(cauchy_pdf(vals, params(1), params(2))));

% % Initial guess: [location, scale]
% init = [median(vals), 1];

% % Fit using fminsearch
% fitted_params = fminsearch(negloglik, init);
% x0 = fitted_params(1);
% gamma = fitted_params(2);

% % Create Cauchy distribution object
% cauchy_dist = makedist('tLocationScale', 'mu', x0, 'sigma', gamma, 'nu', 1);

% % Compute 99th percentile
% perc = icdf(cauchy_dist, 0.99);
% disp(['Cauchy 99th percentile: ', num2str(perc)]);

% % === Annotate thresholds ===
% yline(meanvals + 2 * stdev, '--k', '2\sigma Threshold');
% yline(perc, '--r', '99th Percentile');

% max_val = max([vals(:); raw_y2(:); perc]);
% ylim([0, ceil(max_val) + 2]);  % Add small buffer


% % ylim([0 45]);  % Raised limit to allow full visualization
% title('Difference in Max and Mean Temperature (\DeltaT \circC)');
% ylabel('\DeltaT \circC');
% xlabel('Year');
% legend;
% box on;

% % === Save processed data (optional) ===
% LandsatTable.diff_from_mean = vals;
% writetable(LandsatTable, 'landsattabledata_updated.xlsx');
% writetable(DownscaleTable, 'downscaletabledata_updated.xlsx');
