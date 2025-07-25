% Plotting of thermal data retrieved from downscaling models. 99th percentile value is retreived in this code, however, residual sum of squares calculations
% are done using python instead to make use of the distfit package. This uses the backend of stats packages to test the RSS of every common distribution, 
% it then filters based on the optimum RSS fit of a distribution. With regards to the project the lognormal distribution was found to be the best fit and
% therefore that is why it was used in this research.


% Code to produce temperature time series figures
% Data obtained through thermal imaging scripts presented in other sections
% of the report and/or codebase

LandsatTable = readtable("D:/Dissertation-2542000/RP3/stats8_normal_2015.xlsx")
LandsatTable.Properties.VariableNames
disp("BEFOREE:")
LandsatTable.MaxTemp(1:10)


% Convert 'LandsatAcquisitionDate' from string to datetime format
LandsatTable.('Landsat8AcquisitionDate') = datetime(LandsatTable.('Landsat8AcquisitionDate'), 'InputFormat', 'dd/MM/yyyy');
LandsatTable = sortrows(LandsatTable, 'Landsat8AcquisitionDate');

% Get column values for both maximum and mean temperatures
x1 = LandsatTable.('Landsat8AcquisitionDate');
y1 = LandsatTable.MaxTemp;

% disp("Preview y1 bwginning:"), disp(raw_y1(1:5));
% whos raw_y1

% Step 1: Attempt conversion
% y1 = cellfun(@str2double, raw_y1);
disp("Preview y1 :"), disp(y1(1:5));
% Step 2: Find corrupted (non-convertible) entries
% bad_idx = isnan(y1) & ~cellfun(@(x) any(isnan(str2double(x))), raw_y1);

% Step 3: Display if any issues found
% if any(bad_idx)
%     disp('⚠️ Warning: Some entries could not be converted:');
%     disp(y1(bad_idx));
% else
%     disp('✅ All entries converted successfully');
% end



x1mean = LandsatTable.('Landsat8AcquisitionDate');
y1mean = LandsatTable.MeanTemp;
whos y1_mean

disp("Preview y1mean:"), disp(y1mean(1:5));
% y1mean = cellfun(@str2double, raw_y1mean);
% disp("Preview y1mean after converting:"), disp(y1mean(1:5));

% bad_idx_mean = isnan(y1mean) & ~cellfun(@(x) any(isnan(str2double(x))), raw_y1mean);

% % Step 3: Display if any issues found
% if any(bad_idx_mean)
%     disp('⚠️ Warning: Some entries could not be converted:');
%     disp(y1(bad_idx_mean));
% else
%     disp('✅ All entries converted successfully');
% end


length(x1)
length(y1)

whos x1
whos y1



% % Smoothing the plot to show seasonality
% x1a = [x1(1:36)' datetime('10/01/2018', 'InputFormat', 'dd/MM/yyyy') x1(37:107)'];
% y1a = [y1(1:36)' 20 y1(37:107)'];

% x1a = [x1a(1:43) datetime('10/01/2019', 'InputFormat', 'dd/MM/yyyy') x1a(44:107)];
% y1a = [y1a(1:43) 20 y1a(44:107)];

% disp(x1a)
% disp(y1a)

% if length(x1a) ~= length(y1a)
%     error('x1a and y1a must have the same length');
% end

% Plotting
% plot(x1a', y1a', 'o-');
% hold on

% plot(x1', y1', 'o-');
% hold on

% Loading in downscaled data
DownscaleTable = readtable("D:/Dissertation-2542000/RP3/stats8_downscale.csv");
% Convert 'Landsat 8 acquisition date' from string to datetime format
DownscaleTable.Properties.VariableNames
DownscaleTable.Sentinel2AcquisitionDate = datetime(DownscaleTable.Sentinel2AcquisitionDate, 'InputFormat', 'yyyy/MM/dd');

% Sort the table by 'LandsatAcquisitionDate' column in ascending order
DownscaleTable = sortrows(DownscaleTable, 'Landsat8AcquisitionDate', 'ascend');

% Get the acquisition times of the images so that it can be converted to
% time series
x2 = DownscaleTable.Sentinel2AcquisitionDate
y2 = DownscaleTable.MaxTemp
%plot(DownscaleTable.('Landsat 8 acquisition date'), DownscaleTable.DownscaleMeanTemp, 'xk');

% Interpolating for the seasonality plot. Linear used to show simplistic
% trend, later sine waves are used to demonstrate dynamics of temperature
x1a=x1;
y1a=y1;


datenums = datenum(x1a)
xx=linspace(datenums(1),datenums(end),200);
vq2 = interp1(datenums,y1a',xx,'linear');
t = datetime(xx,'ConvertFrom','datenum')

% Implementation of stl with loess which was not implemented in terms of
% thermal


figure
[LT,ST,R] = trenddecomp(vq2,"stl",30);


figure
plot(x1a,y1a,'or', 'MarkerSize',3,'DisplayName','Landsat 8 (Max. Temp - Mean Temp)')
hold on
plot(x2,y2,'ok', 'MarkerSize',3,'DisplayName','Downscaled (Max. Temp - Mean Temp)')
hold on
ylim([5 75])

whos x1mean
whos y1mean


plot(x1mean, y1 - y1mean, 'or')
plot(x2, y2, 'ok')


% Define the time axis (in years)
t = 0:0.01:3000;

% Define the amplitude and frequency of the sine wave
A = 20;
f = 1/365;

% Calculate the values of the sine wave
y = A*cos(2*pi*f*t) + 36;

% Plot the sine wave
plot(t, y,'--k','DisplayName', 'Seasonality');
legend
title('Maximum Recorded Temperature (ΔT °C)')
ylabel('T °C')
xlabel('Year')

subplot(2,5,[4 5])
plot(t, vq2, 'k')
hold on
plot(x1a,y1a,'or', 'MarkerSize',3)
%plot(t, ST + 25, 'r')
plot(x2,y2,'ok', 'MarkerSize',3)
xlim([t(end)-500, t(end)])
ylim([5 75])

% Figure for plotting the deviations in the maximum recorded downscaled
% temperature and comparing that to the max-mean of the landsat temperature
% over the entire period of suitable imagery.
% Mean and standard deviations also plotted as found from percentile
% functions below and other code from the codebase which computed the
% residual sum of squares of all suitable distributions.
figure
x1mean = LandsatTable.('Landsat8AcquisitionDate');
y1mean = LandsatTable.MeanTemp;
x2 = DownscaleTable.Landsat8AcquisitionDate
raw_y2 = DownscaleTable.MaxTemp - DownscaleTable.MeanTemp

whos raw_y2
% Check sizes
disp("Size y1:"), disp(size(y1));
disp("Size y1mean:"), disp(size(y1mean));

% Check for NaNs
sum(isnan(y1))    % should be 0
sum(isnan(y1mean)) % should be 0

% Check value preview
disp("Preview y1:"), disp(y1(1:5));
disp("Preview y1mean:"), disp(y1mean(1:5));


% y2 = cellfun(@str2double, raw_y2);
hold on
plot(x1mean,y1 - y1mean,'or', 'MarkerSize',3,'DisplayName','Max. - Mean : Landsat 8 ')
plot(x2, raw_y2,'ok', 'MarkerSize',3,'DisplayName','Max. - Mean : Downscale')
vals = y1-y1mean;
meanvals = sum(vals)/108
stdev = std(vals)
yline(9.86,'--')   
yline(15.31,'--')
yline(17.8657,'--')
ylim([0 28])
title('Difference in Maximum and Minimum Recorded Temperature Difference (ΔT °C)')
ylabel('ΔT °C')
xlabel('Year')
box on
legend


% Array handling to transfer the date to a number range since the first
% date acquisition for plotting purposes
y1a = y1a(1,1:107)

datenums = datenum(x1a);

% Calculate the number of days since the first measurement
days = datenums - datenums(1);

days = days(1,1:107)
filename = 'downscaletabledata.xlsx';
% writetable(DownscaleTable,filename,'Sheet',1)

LandsatTable.diff_from_mean = vals;
filename = 'landsattabledata.xlsx';
% writetable(LandsatTable,filename,'Sheet',1)

% Lognormal distribution fitted to observed data values
% 99th percentile taken which is done via the inverse cumulative
% distribution function.
pd = fitdist(vals,'Lognormal')
perc = icdf(pd,0.99)
