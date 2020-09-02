import model.valve as valve
from pkg.common import LinktoInfluxDB
import math, statistics

influxdb_client = LinktoInfluxDB(host='157.230.120.158',
                                 port='8086',
                                 database='m-metrics',
                                 measurement='all_data',
                                 user='admin',
                                 password='admin')

while True:

    query = 'SELECT mean("190-AT2001-AirTemp_(C)") AS "mean_190-AT2001-AirTemp_(C)" FROM "m-metrics"."autogen"."all_data" WHERE time < now()-15s GROUP BY time(15s) FILL(previouse);'

    res = influxdb_client.get_data(query)

    inlet_pressure_a = res.iloc['990-PT-1104A_(barg)'][0]
    inlet_pressure_b = res.iloc['990-PT-1104B_(barg)'][0]
    inlet_pressure_c = res.iloc['990-PT-1104C_(barg)'][0]
    inlet_pressure = statistics.mean([inlet_pressure_a,inlet_pressure_b,inlet_pressure_c])
    outlet_pressure = res.iloc['990-PT-1129_(barg)']
    temperature = 10

    mass_flow = valve.function(P1=inlet_pressure,
                               P2=outlet_pressure,
                               T=temperature)