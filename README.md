# Clusco Dashboard
Dashboard web software designed for LST1 CACO monitoring for [CTA](https://www.cta-observatory.org), which utilizes the [HoloViz](https://holoviz.org) libraries ecosystem.

## 1. Prerequisites
Since this application is based on Python the packages required could be managed using conda. The following is required:

```
- Python 3.10.10+
- Holoviews 1.16
- hvPlot 0.8.3
- Datashader 0.14.4
- Panel 0.14.3
- Bokeh 2.4.3
- Pymongo 3.12.0
- Python-Dotenv 1.0
```

The **yml** file with the conda environment is provided in the repository.

## 2. How to use this repository

**1.** Clone this repository:

```bash
git clone https://github.com/jraicr/clusco-dashboard
```

**2.** Copy the ```.env.example``` file to ```.env``` and edit it to fill the required variables. If you are going to use a domain it is required to fill the ```WEBSOCKET_ORIGIN``` variable with the domain name.

```bash
cp .env.example .env
nano .env
```


**3.** Create a conda environment from the yml file:
```bash
conda env create -f environment.yml
```

**4.** Run the application:
```bash
python app.py
```

By default it will be listening for connections in the port 5006. If you want to change the port you can use the ```-p``` argument.

```bash
python app.py -p 5007
```

Alternatively you may up create a bash script that serves as launcher. This is an example of a bash script that launches the application in the port 7000. (This bash script assumes that you have installed miniconda on your home path)

```bash
#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate holoviz
python -u app.py -p 7000 > output.log
```

And then you can execute the script with nohup, for example, if you wanted to keep it as a background process.

```bash
nohup ./launcher.sh &
```

It is recommended to set up this application behind an Nginx reverse proxy to serve it publicly. Please note that this repository does not provide an example of an Nginx configuration file. However, a sample configuration file could resemble the following:

```nginx
server {
    listen 80;
    server_name dashboard.example.com;

    location / {
        proxy_pass http://localhost:5006;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_http_version 1.1;
    }
}
```

## 3. Usage
Upon running the application, it should be accessible through the browser. If running locally, access it through the address localhost:5006. If running on a remote server, access it through the address or domain configured in your Nginx configuration file.

When a user gets into the application the app will start to query the database for the current day (from 12.00 pm) until the same hour in the next day, in case no data is retrieved it will query the previous day. This will happen successively until it finds a day with data (it will check up to 180 days back). Once the data is retrieved, the application will start to plot the data and will be ready to use.

As the Python application may be used by multiple users simultaneously, it may not always fully clear the Bokeh sessions when a user leaves the application. To address this, there is a functionality that checks every minute if there are any active sessions left once a user leaves the application by closing the browser page. If there are no active sessions, the application will restart, freeing all memory resources that were reserved for the application at the time of dashboard creation.

### 3.1 Observations about date selection
Please note that when selecting a date, the graphs will display data from 12:00 pm on the selected day until 12:00 pm the following day. If you wish to view data from before 12:00 pm on the selected day, you should select the previous day.

## 4 Available plots
The following plots are available in the dashboard:


- PACTA Temperature
- SCB Temperature
- SCB Humidity
- SCB Anode Current
- High Voltage
- SCB Backplane temperature
- L1 Rate
- L0 Pixel IPR
- TIB Rates

 The plots are organized in tabs, each tab contains a set of plots related to a specific topic. The plots are interactive, you can zoom in and out, pan, and hover over the data points to see the values. You can also select a region of the plot to zoom in. As the graphics are based on Bokeh, they are fully interactive.

The color bars in the plots refers to the scattered rasterized values and for the selected channel or module, where we are plotting data points over the line with the proper color.