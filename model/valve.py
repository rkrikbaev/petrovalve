# import the math module  
import math


# valve function
def function(P1,P2,T):

  P1 = P1*100000 + 101325   # Pa, absolut pressure
  P2 = P2*100000 + 101325   # Pa, absolut pressure

  # fludi parameters (water)
  MW = 16.04246          # g/mol, molecular weight
  K  = 1.306150523753    # -    , specific heat coefficient
  Z1 = 0.9866770978914   # -    , compressibility factor ( 5 barg)

  # valve parameters
  Cmd = 1
  CvMAX = 100

  # constant
  Xt = 0.7
  Fp = 1
  N8 = 0.948
  
  # variables unit conversion
  T  = T+273.15   # K
  P1 = P1/1000    # kPa
  P2 = P2/1000    # kPa
  
  Fk     = K / 1.4
  DeltaP = P1 - P2
  
  # A/S e' una check, flusso in una sola direzione
  if DeltaP < 0:
    DeltaP=0

  # Controllo P1
  if P1==0:
    P1=1
    
  x  = DeltaP / P1
  Y  = 1 - x /(3*Fk*Xt)
  Cv = Cmd*CvMAX
  
  massflow = Cv*N8 *Fp*P1*Y*math.sqrt((x*MW)/(T*Z1)) # kg/h
  
  return massflow/3600


# #input
# P1 = 5                    # barg
# P2 = 4                    # barg
# T  = 20                   # °C



# print("\n\n Valve Mass flow [kg/s]")
#
# print(valve_function(P1,P2,MW,K,T,Z1,Cmd,CvMAX))

