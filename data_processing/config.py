from __future__ import annotations

# Matriculation Number: U2222559E
TARGET_YEAR: int = 2019
START_MONTH: int = 5         

MATCHED_TOWNS: set[str] = set([
    "CLEMENTI",
    "JURONG WEST",
    "YISHUN",
])

# valid range of x, y and minimum price per square meter 
X_MIN: int = 1               
X_MAX: int = 8     

Y_MIN: int = 80              
Y_MAX: int = 150     
        
PRICE_PER_SQM_THRESHOLD: float = 4725.0  
