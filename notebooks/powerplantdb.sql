SHOW TABLES FROM power_plant_databank

DESCRIBE TABLE power_plant_databank.generation_supply_curve

SELECT * FROM power_plant_databank.plant_identification

SELECT DISTINCT Country FROM power_plant_databank.plant_location

SELECT pd.PowerPlant, pl.Country, pd.FuelType, pd.OperatingCapacity, pd.OperatingStatus, pd.YearFirstUnitinService
FROM power_plant_databank.plant_identification pi
LEFT JOIN power_plant_databank.plant_location pl ON pi.PlantKey = pl.PlantKey
LEFT JOIN power_plant_databank.plant_description pd ON pi.PlantKey = pd.PlantKey
-- LEFT JOIN plant_development_projects pdp ON pi.PlantKey = pdp.PlantKey
WHERE pd.OperatingStatus = "Operating"

SELECT pl.Country, pd.FuelType, sum(wpom.NetGeneration)
FROM power_plant_databank.plant_identification pi
LEFT JOIN power_plant_databank.plant_location pl ON pi.PlantKey = pl.PlantKey
LEFT JOIN power_plant_databank.plant_description pd ON pi.PlantKey = pd.PlantKey
LEFT JOIN power_plant_databank.whole_plant_operating_annual wpom ON wpom.PlantKey = pi.PlantKey
-- LEFT JOIN plant_development_projects pdp ON pi.PlantKey = pdp.PlantKey
WHERE pd.OperatingStatus = "Operating"
GROUP BY pl.Country, pd.FuelType