import os
import csv
import re
import datetime
import calendar
import time
from tables import *
import numpy as np


# # open the data file in "w"rite mode
# filename = 'monitoring-data.h5'
# h5file = open_file(filename, mode = 'w', title = 'PQube monitoring data file')


class Monitoring(IsDescription):
    date = Time32Col(pos=0)
    batch = Int32Col(pos=1)
    flags = StringCol(8, pos=2)
    L_N_RMS_1_2__1_cyc_Min_V = Float32Col()
    L_N_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L_N_RMS_1_2__1_cyc_Max_V = Float32Col()
    L_N_RMS_10_Cycle_Min_V = Float32Col()
    L_N_RMS_10_Cycle_Avg_V = Float32Col()
    L_N_RMS_10_Cycle_Max_V = Float32Col()
    L_L_RMS_1_2__1_cyc_Min_V = Float32Col()
    L_L_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L_L_RMS_1_2__1_cyc_Max_V = Float32Col()
    L_L_RMS_10_Cycle_Min_V = Float32Col()
    L_L_RMS_10_Cycle_Avg_V = Float32Col()
    L_L_RMS_10_Cycle_Max_V = Float32Col()
    Current_RMS_1_2__1_cyc_Min_A = Float32Col()
    Current_RMS_1_2__1_cyc_Avg_A = Float32Col()
    Current_RMS_1_2__1_cyc_Max_A = Float32Col()
    Current_RMS_10_Cycle_Min_A = Float32Col()
    Current_RMS_10_Cycle_Avg_A = Float32Col()
    Current_RMS_10_Cycle_Max_A = Float32Col()
    Frequency__1_cyc_Min_Hz = Float32Col()
    Frequency__1_cyc_Avg_Hz = Float32Col()
    Frequency__1_cyc_Max_Hz = Float32Col()
    THD_V_Min_perc = Float32Col()
    THD_V_Avg_perc = Float32Col()
    THD_V_Max_perc = Float32Col()
    TDD_A_Min_perc = Float32Col()
    TDD_A_Avg_perc = Float32Col()
    TDD_A_Max_perc = Float32Col()
    IEC_Negative_Sequence_V_Min_perc = Float32Col()
    IEC_Negative_Sequence_V_Avg_perc = Float32Col()
    IEC_Negative_Sequence_V_Max_perc = Float32Col()
    IEC_Zero_Sequence_V_Min_perc = Float32Col()
    IEC_Zero_Sequence_V_Avg_perc = Float32Col()
    IEC_Zero_Sequence_V_Max_perc = Float32Col()
    IEC_Negative_Sequence_A_Min_perc = Float32Col()
    IEC_Negative_Sequence_A_Avg_perc = Float32Col()
    IEC_Negative_Sequence_A_Max_perc = Float32Col()
    IEC_Zero_Sequence_A_Min_perc = Float32Col()
    IEC_Zero_Sequence_A_Avg_perc = Float32Col()
    IEC_Zero_Sequence_A_Max_perc = Float32Col()
    Flicker_P_inst_Min = Float32Col()
    Flicker_P_inst_Avg = Float32Col()
    Flicker_P_inst_Max = Float32Col()
    Flicker_P_st_Min = Float32Col()
    Flicker_P_st_Avg = Float32Col()
    Flicker_P_st_Max = Float32Col()
    Flicker_P_lt_Min = Float32Col()
    Flicker_P_lt_Avg = Float32Col()
    Flicker_P_lt_Max = Float32Col()
    N_E_RMS_1_2__1_cyc_Min_V = Float32Col()
    N_E_RMS_1_2__1_cyc_Avg_V = Float32Col()
    N_E_RMS_1_2__1_cyc_Max_V = Float32Col()
    L1_N_RMS_1_2__1_cyc_Min_V = Float32Col()
    L1_N_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L1_N_RMS_1_2__1_cyc_Max_V = Float32Col()
    L2_N_RMS_1_2__1_cyc_Min_V = Float32Col()
    L2_N_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L2_N_RMS_1_2__1_cyc_Max_V = Float32Col()
    L3_N_RMS_1_2__1_cyc_Min_V = Float32Col()
    L3_N_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L3_N_RMS_1_2__1_cyc_Max_V = Float32Col()
    L1_L2_RMS_1_2__1_cyc_Min_V = Float32Col()
    L1_L2_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L1_L2_RMS_1_2__1_cyc_Max_V = Float32Col()
    L2_L3_RMS_1_2__1_cyc_Min_V = Float32Col()
    L2_L3_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L2_L3_RMS_1_2__1_cyc_Max_V = Float32Col()
    L3_L1_RMS_1_2__1_cyc_Min_V = Float32Col()
    L3_L1_RMS_1_2__1_cyc_Avg_V = Float32Col()
    L3_L1_RMS_1_2__1_cyc_Max_V = Float32Col()
    L1_Current_RMS_1_2__1_cyc_Min_A = Float32Col()
    L1_Current_RMS_1_2__1_cyc_Avg_A = Float32Col()
    L1_Current_RMS_1_2__1_cyc_Max_A = Float32Col()
    L2_Current_RMS_1_2__1_cyc_Min_A = Float32Col()
    L2_Current_RMS_1_2__1_cyc_Avg_A = Float32Col()
    L2_Current_RMS_1_2__1_cyc_Max_A = Float32Col()
    L3_Current_RMS_1_2__1_cyc_Min_A = Float32Col()
    L3_Current_RMS_1_2__1_cyc_Avg_A = Float32Col()
    L3_Current_RMS_1_2__1_cyc_Max_A = Float32Col()
    N_Current_RMS_1_2__1_cyc_Min_A = Float32Col()
    N_Current_RMS_1_2__1_cyc_Avg_A = Float32Col()
    N_Current_RMS_1_2__1_cyc_Max_A = Float32Col()
    E_Current_RMS_1_2__1_cyc_Min_A = Float32Col()
    E_Current_RMS_1_2__1_cyc_Avg_A = Float32Col()
    E_Current_RMS_1_2__1_cyc_Max_A = Float32Col()
    THD_V_L1_Min_perc = Float32Col()
    THD_V_L1_Avg_perc = Float32Col()
    THD_V_L1_Max_perc = Float32Col()
    THD_V_L2_Min_perc = Float32Col()
    THD_V_L2_Avg_perc = Float32Col()
    THD_V_L2_Max_perc = Float32Col()
    THD_V_L3_Min_perc = Float32Col()
    THD_V_L3_Avg_perc = Float32Col()
    THD_V_L3_Max_perc = Float32Col()
    TDD_A_L1_Min_perc = Float32Col()
    TDD_A_L1_Avg_perc = Float32Col()
    TDD_A_L1_Max_perc = Float32Col()
    TDD_A_L2_Min_perc = Float32Col()
    TDD_A_L2_Avg_perc = Float32Col()
    TDD_A_L2_Max_perc = Float32Col()
    TDD_A_L3_Min_perc = Float32Col()
    TDD_A_L3_Avg_perc = Float32Col()
    TDD_A_L3_Max_perc = Float32Col()
    P_inst_L1_Min = Float32Col()
    P_inst_L1_Avg = Float32Col()
    P_inst_L1_Max = Float32Col()
    P_inst_L2_Min = Float32Col()
    P_inst_L2_Avg = Float32Col()
    P_inst_L2_Max = Float32Col()
    P_inst_L3_Min = Float32Col()
    P_inst_L3_Avg = Float32Col()
    P_inst_L3_Max = Float32Col()
    P_st_L1_Min = Float32Col()
    P_st_L1_Avg = Float32Col()
    P_st_L1_Max = Float32Col()
    P_st_L2_Min = Float32Col()
    P_st_L2_Avg = Float32Col()
    P_st_L2_Max = Float32Col()
    P_st_L3_Min = Float32Col()
    P_st_L3_Avg = Float32Col()
    P_st_L3_Max = Float32Col()
    P_lt_L1_Min = Float32Col()
    P_lt_L1_Avg = Float32Col()
    P_lt_L1_Max = Float32Col()
    P_lt_L2_Min = Float32Col()
    P_lt_L2_Avg = Float32Col()
    P_lt_L2_Max = Float32Col()
    P_lt_L3_Min = Float32Col()
    P_lt_L3_Avg = Float32Col()
    P_lt_L3_Max = Float32Col()
    Apparent_Power_10_Cycle_Min_kVA = Float32Col()
    Apparent_Power_10_Cycle_Avg_kVA = Float32Col()
    Apparent_Power_10_Cycle_Max_kVA = Float32Col()
    Reactive_Power_10_Cycle_Min_kVAR = Float32Col()
    Reactive_Power_10_Cycle_Avg_kVAR = Float32Col()
    Reactive_Power_10_Cycle_Max_kVAR = Float32Col()
    Real_Power_10_Cycle_Min_kW = Float32Col()
    Real_Power_10_Cycle_Avg_kW = Float32Col()
    Real_Power_10_Cycle_Max_kW = Float32Col()
    tPF_10_Cycle_Min = Float32Col()
    tPF_10_Cycle_Avg = Float32Col()
    tPF_10_Cycle_Max = Float32Col()
    Carbon_Rate_10_Cycle_Min_kg_h = Float32Col()
    Carbon_Rate_10_Cycle_Avg_kg_h = Float32Col()
    Carbon_Rate_10_Cycle_Max_kg_h = Float32Col()
    VA_L1_10_Cycle_Min_kVA = Float32Col()
    VA_L1_10_Cycle_Avg_kVA = Float32Col()
    VA_L1_10_Cycle_Max_kVA = Float32Col()
    VA_L2_10_Cycle_Min_kVA = Float32Col()
    VA_L2_10_Cycle_Avg_kVA = Float32Col()
    VA_L2_10_Cycle_Max_kVA = Float32Col()
    VA_L3_10_Cycle_Min_kVA = Float32Col()
    VA_L3_10_Cycle_Avg_kVA = Float32Col()
    VA_L3_10_Cycle_Max_kVA = Float32Col()
    VAR_L1_10_Cycle_Min_kVAR = Float32Col()
    VAR_L1_10_Cycle_Avg_kVAR = Float32Col()
    VAR_L1_10_Cycle_Max_kVAR = Float32Col()
    VAR_L2_10_Cycle_Min_kVAR = Float32Col()
    VAR_L2_10_Cycle_Avg_kVAR = Float32Col()
    VAR_L2_10_Cycle_Max_kVAR = Float32Col()
    VAR_L3_10_Cycle_Min_kVAR = Float32Col()
    VAR_L3_10_Cycle_Avg_kVAR = Float32Col()
    VAR_L3_10_Cycle_Max_kVAR = Float32Col()
    Real_Power_L1_10_Cycle_Min_kW = Float32Col()
    Real_Power_L1_10_Cycle_Avg_kW = Float32Col()
    Real_Power_L1_10_Cycle_Max_kW = Float32Col()
    Real_Power_L2_10_Cycle_Min_kW = Float32Col()
    Real_Power_L2_10_Cycle_Avg_kW = Float32Col()
    Real_Power_L2_10_Cycle_Max_kW = Float32Col()
    Real_Power_L3_10_Cycle_Min_kW = Float32Col()
    Real_Power_L3_10_Cycle_Avg_kW = Float32Col()
    Real_Power_L3_10_Cycle_Max_kW = Float32Col()
    tPF_L1_10_Cycle_Min = Float32Col()
    tPF_L1_10_Cycle_Avg = Float32Col()
    tPF_L1_10_Cycle_Max = Float32Col()
    tPF_L2_10_Cycle_Min = Float32Col()
    tPF_L2_10_Cycle_Avg = Float32Col()
    tPF_L2_10_Cycle_Max = Float32Col()
    tPF_L3_10_Cycle_Min = Float32Col()
    tPF_L3_10_Cycle_Avg = Float32Col()
    tPF_L3_10_Cycle_Max = Float32Col()
    Energy__kWH = Float32Col()

class Harmonics(IsDescription):
    date = Time32Col(pos=0)
    batch = Int32Col(pos=1)
    H0_mag = Float32Col(pos=2)
    H0_ang = Float32Col(pos=3)
    H0_inter_mag = Float32Col(pos=4)
    H1_mag = Float32Col(pos=5)
    H1_ang = Float32Col(pos=6)
    H1_inter_mag = Float32Col(pos=7)
    H2_mag = Float32Col(pos=8)
    H2_ang = Float32Col(pos=9)
    H2_inter_mag = Float32Col(pos=10)
    H3_mag = Float32Col(pos=11)
    H3_ang = Float32Col(pos=12)
    H3_inter_mag = Float32Col(pos=13)
    H4_mag = Float32Col(pos=14)
    H4_ang = Float32Col(pos=15)
    H4_inter_mag = Float32Col(pos=16)
    H5_mag = Float32Col(pos=17)
    H5_ang = Float32Col(pos=18)
    H5_inter_mag = Float32Col(pos=19)
    H6_mag = Float32Col(pos=20)
    H6_ang = Float32Col(pos=21)
    H6_inter_mag = Float32Col(pos=22)
    H7_mag = Float32Col(pos=23)
    H7_ang = Float32Col(pos=24)
    H7_inter_mag = Float32Col(pos=25)
    H8_mag = Float32Col(pos=26)
    H8_ang = Float32Col(pos=27)
    H8_inter_mag = Float32Col(pos=28)
    H9_mag = Float32Col(pos=29)
    H9_ang = Float32Col(pos=30)
    H9_inter_mag = Float32Col(pos=31)
    H10_mag = Float32Col(pos=32)
    H10_ang = Float32Col(pos=33)
    H10_inter_mag = Float32Col(pos=34)
    H11_mag = Float32Col(pos=35)
    H11_ang = Float32Col(pos=36)
    H11_inter_mag = Float32Col(pos=37)
    H12_mag = Float32Col(pos=38)
    H12_ang = Float32Col(pos=39)
    H12_inter_mag = Float32Col(pos=40)
    H13_mag = Float32Col(pos=41)
    H13_ang = Float32Col(pos=42)
    H13_inter_mag = Float32Col(pos=43)
    H14_mag = Float32Col(pos=44)
    H14_ang = Float32Col(pos=45)
    H14_inter_mag = Float32Col(pos=46)
    H15_mag = Float32Col(pos=47)
    H15_ang = Float32Col(pos=48)
    H15_inter_mag = Float32Col(pos=49)
    H16_mag = Float32Col(pos=50)
    H16_ang = Float32Col(pos=51)
    H16_inter_mag = Float32Col(pos=52)
    H17_mag = Float32Col(pos=53)
    H17_ang = Float32Col(pos=54)
    H17_inter_mag = Float32Col(pos=55)
    H18_mag = Float32Col(pos=56)
    H18_ang = Float32Col(pos=57)
    H18_inter_mag = Float32Col(pos=58)
    H19_mag = Float32Col(pos=59)
    H19_ang = Float32Col(pos=60)
    H19_inter_mag = Float32Col(pos=61)
    H20_mag = Float32Col(pos=62)
    H20_ang = Float32Col(pos=63)
    H20_inter_mag = Float32Col(pos=64)
    H21_mag = Float32Col(pos=65)
    H21_ang = Float32Col(pos=66)
    H21_inter_mag = Float32Col(pos=67)
    H22_mag = Float32Col(pos=68)
    H22_ang = Float32Col(pos=69)
    H22_inter_mag = Float32Col(pos=70)
    H23_mag = Float32Col(pos=71)
    H23_ang = Float32Col(pos=72)
    H23_inter_mag = Float32Col(pos=73)
    H24_mag = Float32Col(pos=74)
    H24_ang = Float32Col(pos=75)
    H24_inter_mag = Float32Col(pos=76)
    H25_mag = Float32Col(pos=77)
    H25_ang = Float32Col(pos=78)
    H25_inter_mag = Float32Col(pos=79)
    H26_mag = Float32Col(pos=80)
    H26_ang = Float32Col(pos=81)
    H26_inter_mag = Float32Col(pos=82)
    H27_mag = Float32Col(pos=83)
    H27_ang = Float32Col(pos=84)
    H27_inter_mag = Float32Col(pos=85)
    H28_mag = Float32Col(pos=86)
    H28_ang = Float32Col(pos=87)
    H28_inter_mag = Float32Col(pos=88)
    H29_mag = Float32Col(pos=89)
    H29_ang = Float32Col(pos=90)
    H29_inter_mag = Float32Col(pos=91)
    H30_mag = Float32Col(pos=92)
    H30_ang = Float32Col(pos=93)
    H30_inter_mag = Float32Col(pos=94)
    H31_mag = Float32Col(pos=95)
    H31_ang = Float32Col(pos=96)
    H31_inter_mag = Float32Col(pos=97)
    H32_mag = Float32Col(pos=98)
    H32_ang = Float32Col(pos=99)
    H32_inter_mag = Float32Col(pos=100)
    H33_mag = Float32Col(pos=101)
    H33_ang = Float32Col(pos=102)
    H33_inter_mag = Float32Col(pos=103)
    H34_mag = Float32Col(pos=104)
    H34_ang = Float32Col(pos=105)
    H34_inter_mag = Float32Col(pos=106)
    H35_mag = Float32Col(pos=107)
    H35_ang = Float32Col(pos=108)
    H35_inter_mag = Float32Col(pos=109)
    H36_mag = Float32Col(pos=110)
    H36_ang = Float32Col(pos=111)
    H36_inter_mag = Float32Col(pos=112)
    H37_mag = Float32Col(pos=113)
    H37_ang = Float32Col(pos=114)
    H37_inter_mag = Float32Col(pos=115)
    H38_mag = Float32Col(pos=116)
    H38_ang = Float32Col(pos=117)
    H38_inter_mag = Float32Col(pos=118)
    H39_mag = Float32Col(pos=119)
    H39_ang = Float32Col(pos=120)
    H39_inter_mag = Float32Col(pos=121)
    H40_mag = Float32Col(pos=122)
    H40_ang = Float32Col(pos=123)
    H40_inter_mag = Float32Col(pos=124)
    H41_mag = Float32Col(pos=125)
    H41_ang = Float32Col(pos=126)
    H41_inter_mag = Float32Col(pos=127)
    H42_mag = Float32Col(pos=128)
    H42_ang = Float32Col(pos=129)
    H42_inter_mag = Float32Col(pos=130)
    H43_mag = Float32Col(pos=131)
    H43_ang = Float32Col(pos=132)
    H43_inter_mag = Float32Col(pos=133)
    H44_mag = Float32Col(pos=134)
    H44_ang = Float32Col(pos=135)
    H44_inter_mag = Float32Col(pos=136)
    H45_mag = Float32Col(pos=137)
    H45_ang = Float32Col(pos=138)
    H45_inter_mag = Float32Col(pos=139)
    H46_mag = Float32Col(pos=140)
    H46_ang = Float32Col(pos=141)
    H46_inter_mag = Float32Col(pos=142)
    H47_mag = Float32Col(pos=143)
    H47_ang = Float32Col(pos=144)
    H47_inter_mag = Float32Col(pos=145)
    H48_mag = Float32Col(pos=146)
    H48_ang = Float32Col(pos=147)
    H48_inter_mag = Float32Col(pos=148)
    H49_mag = Float32Col(pos=149)
    H49_ang = Float32Col(pos=150)
    H49_inter_mag = Float32Col(pos=151)
    H50_mag = Float32Col(pos=152)
    H50_ang = Float32Col(pos=153)
    H50_inter_mag = Float32Col(pos=154)
    H51_mag = Float32Col(pos=155)
    H51_ang = Float32Col(pos=156)
    H51_inter_mag = Float32Col(pos=157)
    H52_mag = Float32Col(pos=158)
    H52_ang = Float32Col(pos=159)
    H52_inter_mag = Float32Col(pos=160)
    H53_mag = Float32Col(pos=161)
    H53_ang = Float32Col(pos=162)
    H53_inter_mag = Float32Col(pos=163)
    H54_mag = Float32Col(pos=164)
    H54_ang = Float32Col(pos=165)
    H54_inter_mag = Float32Col(pos=166)
    H55_mag = Float32Col(pos=167)
    H55_ang = Float32Col(pos=168)
    H55_inter_mag = Float32Col(pos=169)
    H56_mag = Float32Col(pos=170)
    H56_ang = Float32Col(pos=171)
    H56_inter_mag = Float32Col(pos=172)
    H57_mag = Float32Col(pos=173)
    H57_ang = Float32Col(pos=174)
    H57_inter_mag = Float32Col(pos=175)
    H58_mag = Float32Col(pos=176)
    H58_ang = Float32Col(pos=177)
    H58_inter_mag = Float32Col(pos=178)
    H59_mag = Float32Col(pos=179)
    H59_ang = Float32Col(pos=180)
    H59_inter_mag = Float32Col(pos=181)
    H60_mag = Float32Col(pos=182)
    H60_ang = Float32Col(pos=183)
    H60_inter_mag = Float32Col(pos=184)
    H61_mag = Float32Col(pos=185)
    H61_ang = Float32Col(pos=186)
    H61_inter_mag = Float32Col(pos=187)
    H62_mag = Float32Col(pos=188)
    H62_ang = Float32Col(pos=189)
    H62_inter_mag = Float32Col(pos=190)
    H63_mag = Float32Col(pos=191)
    H63_ang = Float32Col(pos=192)
    H63_inter_mag = Float32Col(pos=193)


class Events(IsDescription):
    date = Time32Col(pos=0)
    batch = Int32Col(pos=1)
    two_part = BoolCol(pos=2)
    duration_ms = Float32Col(pos=3)
    Event_Type = StringCol(64, pos=4)
    Trigger_Channel = StringCol(16, pos=5)
    Trigger_Sample_Number = Int32Col(pos=6)
    Samples_Per_Cycle = Int32Col(pos=7)
    Microseconds_Per_Sample = Float32Col(pos=8)
    Milliseconds = Float32Col(2048, pos=9)
    N_E__V = Float32Col(2048, pos=10)
    L1_N__V = Float32Col(2048, pos=11)
    L2_N__V = Float32Col(2048, pos=12)
    L3_N__V = Float32Col(2048, pos=13)
    L1_Amp__A = Float32Col(2048, pos=14)
    L2_Amp__A = Float32Col(2048, pos=15)
    L3_Amp__A = Float32Col(2048, pos=16)
    N_Amp__A = Float32Col(2048, pos=17)


CSV_labels = {
    'L_N_RMS_1_2__1_cyc_Min_V': 'L-N RMS 1/2 (1-cyc) Min(V)', 
    'L_N_RMS_1_2__1_cyc_Avg_V': 'L-N RMS 1/2 (1-cyc) Avg(V)', 
    'L_N_RMS_1_2__1_cyc_Max_V': 'L-N RMS 1/2 (1-cyc) Max(V)', 
    'L_N_RMS_10_Cycle_Min_V': 'L-N RMS 10-Cycle Min(V)', 
    'L_N_RMS_10_Cycle_Avg_V': 'L-N RMS 10-Cycle Avg(V)', 
    'L_N_RMS_10_Cycle_Max_V': 'L-N RMS 10-Cycle Max(V)', 
    'L_L_RMS_1_2__1_cyc_Min_V': 'L-L RMS 1/2 (1-cyc) Min(V)', 
    'L_L_RMS_1_2__1_cyc_Avg_V': 'L-L RMS 1/2 (1-cyc) Avg(V)', 
    'L_L_RMS_1_2__1_cyc_Max_V': 'L-L RMS 1/2 (1-cyc) Max(V)', 
    'L_L_RMS_10_Cycle_Min_V': 'L-L RMS 10-Cycle Min(V)', 
    'L_L_RMS_10_Cycle_Avg_V': 'L-L RMS 10-Cycle Avg(V)', 
    'L_L_RMS_10_Cycle_Max_V': 'L-L RMS 10-Cycle Max(V)', 
    'Current_RMS_1_2__1_cyc_Min_A': 'Current RMS 1/2 (1-cyc) Min(A)', 
    'Current_RMS_1_2__1_cyc_Avg_A': 'Current RMS 1/2 (1-cyc) Avg(A)', 
    'Current_RMS_1_2__1_cyc_Max_A': 'Current RMS 1/2 (1-cyc) Max(A)', 
    'Current_RMS_10_Cycle_Min_A': 'Current RMS 10-Cycle Min(A)', 
    'Current_RMS_10_Cycle_Avg_A': 'Current RMS 10-Cycle Avg(A)', 
    'Current_RMS_10_Cycle_Max_A': 'Current RMS 10-Cycle Max(A)', 
    'Frequency__1_cyc_Min_Hz': 'Frequency (1-cyc) Min(Hz)', 
    'Frequency__1_cyc_Avg_Hz': 'Frequency (1-cyc) Avg(Hz)', 
    'Frequency__1_cyc_Max_Hz': 'Frequency (1-cyc) Max(Hz)', 
    'THD_V_Min_perc': 'THD-V Min(%)', 
    'THD_V_Avg_perc': 'THD-V Avg(%)', 
    'THD_V_Max_perc': 'THD-V Max(%)', 
    'TDD_A_Min_perc': 'TDD-A Min(%)', 
    'TDD_A_Avg_perc': 'TDD-A Avg(%)', 
    'TDD_A_Max_perc': 'TDD-A Max(%)', 
    'IEC_Negative_Sequence_V_Min_perc': 'IEC Negative Sequence V Min(%)', 
    'IEC_Negative_Sequence_V_Avg_perc': 'IEC Negative Sequence V Avg(%)', 
    'IEC_Negative_Sequence_V_Max_perc': 'IEC Negative Sequence V Max(%)', 
    'IEC_Zero_Sequence_V_Min_perc': 'IEC Zero Sequence V Min(%)', 
    'IEC_Zero_Sequence_V_Avg_perc': 'IEC Zero Sequence V Avg(%)', 
    'IEC_Zero_Sequence_V_Max_perc': 'IEC Zero Sequence V Max(%)', 
    'IEC_Negative_Sequence_A_Min_perc': 'IEC Negative Sequence A Min(%)', 
    'IEC_Negative_Sequence_A_Avg_perc': 'IEC Negative Sequence A Avg(%)', 
    'IEC_Negative_Sequence_A_Max_perc': 'IEC Negative Sequence A Max(%)', 
    'IEC_Zero_Sequence_A_Min_perc': 'IEC Zero Sequence A Min(%)', 
    'IEC_Zero_Sequence_A_Avg_perc': 'IEC Zero Sequence A Avg(%)', 
    'IEC_Zero_Sequence_A_Max_perc': 'IEC Zero Sequence A Max(%)', 
    'Flicker_P_inst_Min': 'Flicker P(inst) Min', 
    'Flicker_P_inst_Avg': 'Flicker P(inst) Avg', 
    'Flicker_P_inst_Max': 'Flicker P(inst) Max', 
    'Flicker_P_st_Min': 'Flicker P(st) Min', 
    'Flicker_P_st_Avg': 'Flicker P(st) Avg', 
    'Flicker_P_st_Max': 'Flicker P(st) Max', 
    'Flicker_P_lt_Min': 'Flicker P(lt) Min', 
    'Flicker_P_lt_Avg': 'Flicker P(lt) Avg', 
    'Flicker_P_lt_Max': 'Flicker P(lt) Max', 
    'N_E_RMS_1_2__1_cyc_Min_V': 'N-E RMS 1/2 (1-cyc) Min(V)', 
    'N_E_RMS_1_2__1_cyc_Avg_V': 'N-E RMS 1/2 (1-cyc) Avg(V)', 
    'N_E_RMS_1_2__1_cyc_Max_V': 'N-E RMS 1/2 (1-cyc) Max(V)', 
    'L1_N_RMS_1_2__1_cyc_Min_V': 'L1-N RMS 1/2 (1-cyc) Min(V)', 
    'L1_N_RMS_1_2__1_cyc_Avg_V': 'L1-N RMS 1/2 (1-cyc) Avg(V)', 
    'L1_N_RMS_1_2__1_cyc_Max_V': 'L1-N RMS 1/2 (1-cyc) Max(V)', 
    'L2_N_RMS_1_2__1_cyc_Min_V': 'L2-N RMS 1/2 (1-cyc) Min(V)', 
    'L2_N_RMS_1_2__1_cyc_Avg_V': 'L2-N RMS 1/2 (1-cyc) Avg(V)', 
    'L2_N_RMS_1_2__1_cyc_Max_V': 'L2-N RMS 1/2 (1-cyc) Max(V)', 
    'L3_N_RMS_1_2__1_cyc_Min_V': 'L3-N RMS 1/2 (1-cyc) Min(V)', 
    'L3_N_RMS_1_2__1_cyc_Avg_V': 'L3-N RMS 1/2 (1-cyc) Avg(V)', 
    'L3_N_RMS_1_2__1_cyc_Max_V': 'L3-N RMS 1/2 (1-cyc) Max(V)', 
    'L1_L2_RMS_1_2__1_cyc_Min_V': 'L1-L2 RMS 1/2 (1-cyc) Min(V)', 
    'L1_L2_RMS_1_2__1_cyc_Avg_V': 'L1-L2 RMS 1/2 (1-cyc) Avg(V)', 
    'L1_L2_RMS_1_2__1_cyc_Max_V': 'L1-L2 RMS 1/2 (1-cyc) Max(V)', 
    'L2_L3_RMS_1_2__1_cyc_Min_V': 'L2-L3 RMS 1/2 (1-cyc) Min(V)', 
    'L2_L3_RMS_1_2__1_cyc_Avg_V': 'L2-L3 RMS 1/2 (1-cyc) Avg(V)', 
    'L2_L3_RMS_1_2__1_cyc_Max_V': 'L2-L3 RMS 1/2 (1-cyc) Max(V)', 
    'L3_L1_RMS_1_2__1_cyc_Min_V': 'L3-L1 RMS 1/2 (1-cyc) Min(V)', 
    'L3_L1_RMS_1_2__1_cyc_Avg_V': 'L3-L1 RMS 1/2 (1-cyc) Avg(V)', 
    'L3_L1_RMS_1_2__1_cyc_Max_V': 'L3-L1 RMS 1/2 (1-cyc) Max(V)', 
    'L1_Current_RMS_1_2__1_cyc_Min_A': 'L1 Current RMS 1/2 (1-cyc) Min(A)', 
    'L1_Current_RMS_1_2__1_cyc_Avg_A': 'L1 Current RMS 1/2 (1-cyc) Avg(A)', 
    'L1_Current_RMS_1_2__1_cyc_Max_A': 'L1 Current RMS 1/2 (1-cyc) Max(A)', 
    'L2_Current_RMS_1_2__1_cyc_Min_A': 'L2 Current RMS 1/2 (1-cyc) Min(A)', 
    'L2_Current_RMS_1_2__1_cyc_Avg_A': 'L2 Current RMS 1/2 (1-cyc) Avg(A)', 
    'L2_Current_RMS_1_2__1_cyc_Max_A': 'L2 Current RMS 1/2 (1-cyc) Max(A)', 
    'L3_Current_RMS_1_2__1_cyc_Min_A': 'L3 Current RMS 1/2 (1-cyc) Min(A)', 
    'L3_Current_RMS_1_2__1_cyc_Avg_A': 'L3 Current RMS 1/2 (1-cyc) Avg(A)', 
    'L3_Current_RMS_1_2__1_cyc_Max_A': 'L3 Current RMS 1/2 (1-cyc) Max(A)', 
    'N_Current_RMS_1_2__1_cyc_Min_A': 'N Current RMS 1/2 (1-cyc) Min(A)', 
    'N_Current_RMS_1_2__1_cyc_Avg_A': 'N Current RMS 1/2 (1-cyc) Avg(A)', 
    'N_Current_RMS_1_2__1_cyc_Max_A': 'N Current RMS 1/2 (1-cyc) Max(A)', 
    'E_Current_RMS_1_2__1_cyc_Min_A': 'E Current RMS 1/2 (1-cyc) Min(A)', 
    'E_Current_RMS_1_2__1_cyc_Avg_A': 'E Current RMS 1/2 (1-cyc) Avg(A)', 
    'E_Current_RMS_1_2__1_cyc_Max_A': 'E Current RMS 1/2 (1-cyc) Max(A)', 
    'THD_V_L1_Min_perc': 'THD-V L1 Min(%)', 
    'THD_V_L1_Avg_perc': 'THD-V L1 Avg(%)', 
    'THD_V_L1_Max_perc': 'THD-V L1 Max(%)', 
    'THD_V_L2_Min_perc': 'THD-V L2 Min(%)', 
    'THD_V_L2_Avg_perc': 'THD-V L2 Avg(%)', 
    'THD_V_L2_Max_perc': 'THD-V L2 Max(%)', 
    'THD_V_L3_Min_perc': 'THD-V L3 Min(%)', 
    'THD_V_L3_Avg_perc': 'THD-V L3 Avg(%)', 
    'THD_V_L3_Max_perc': 'THD-V L3 Max(%)', 
    'TDD_A_L1_Min_perc': 'TDD-A L1 Min(%)', 
    'TDD_A_L1_Avg_perc': 'TDD-A L1 Avg(%)', 
    'TDD_A_L1_Max_perc': 'TDD-A L1 Max(%)', 
    'TDD_A_L2_Min_perc': 'TDD-A L2 Min(%)', 
    'TDD_A_L2_Avg_perc': 'TDD-A L2 Avg(%)', 
    'TDD_A_L2_Max_perc': 'TDD-A L2 Max(%)', 
    'TDD_A_L3_Min_perc': 'TDD-A L3 Min(%)', 
    'TDD_A_L3_Avg_perc': 'TDD-A L3 Avg(%)', 
    'TDD_A_L3_Max_perc': 'TDD-A L3 Max(%)', 
    'P_inst_L1_Min': 'P(inst) L1 Min', 
    'P_inst_L1_Avg': 'P(inst) L1 Avg', 
    'P_inst_L1_Max': 'P(inst) L1 Max', 
    'P_inst_L2_Min': 'P(inst) L2 Min', 
    'P_inst_L2_Avg': 'P(inst) L2 Avg', 
    'P_inst_L2_Max': 'P(inst) L2 Max', 
    'P_inst_L3_Min': 'P(inst) L3 Min', 
    'P_inst_L3_Avg': 'P(inst) L3 Avg', 
    'P_inst_L3_Max': 'P(inst) L3 Max', 
    'P_st_L1_Min': 'P(st) L1 Min', 
    'P_st_L1_Avg': 'P(st) L1 Avg', 
    'P_st_L1_Max': 'P(st) L1 Max', 
    'P_st_L2_Min': 'P(st) L2 Min', 
    'P_st_L2_Avg': 'P(st) L2 Avg', 
    'P_st_L2_Max': 'P(st) L2 Max', 
    'P_st_L3_Min': 'P(st) L3 Min', 
    'P_st_L3_Avg': 'P(st) L3 Avg', 
    'P_st_L3_Max': 'P(st) L3 Max', 
    'P_lt_L1_Min': 'P(lt) L1 Min', 
    'P_lt_L1_Avg': 'P(lt) L1 Avg', 
    'P_lt_L1_Max': 'P(lt) L1 Max', 
    'P_lt_L2_Min': 'P(lt) L2 Min', 
    'P_lt_L2_Avg': 'P(lt) L2 Avg', 
    'P_lt_L2_Max': 'P(lt) L2 Max', 
    'P_lt_L3_Min': 'P(lt) L3 Min', 
    'P_lt_L3_Avg': 'P(lt) L3 Avg', 
    'P_lt_L3_Max': 'P(lt) L3 Max', 
    'Apparent_Power_10_Cycle_Min_kVA': 'Apparent Power 10-Cycle Min(kVA)', 
    'Apparent_Power_10_Cycle_Avg_kVA': 'Apparent Power 10-Cycle Avg(kVA)', 
    'Apparent_Power_10_Cycle_Max_kVA': 'Apparent Power 10-Cycle Max(kVA)', 
    'Reactive_Power_10_Cycle_Min_kVAR': 'Reactive Power 10-Cycle Min(kVAR)', 
    'Reactive_Power_10_Cycle_Avg_kVAR': 'Reactive Power 10-Cycle Avg(kVAR)', 
    'Reactive_Power_10_Cycle_Max_kVAR': 'Reactive Power 10-Cycle Max(kVAR)', 
    'Real_Power_10_Cycle_Min_kW': 'Real Power 10-Cycle Min(kW)', 
    'Real_Power_10_Cycle_Avg_kW': 'Real Power 10-Cycle Avg(kW)', 
    'Real_Power_10_Cycle_Max_kW': 'Real Power 10-Cycle Max(kW)', 
    'tPF_10_Cycle_Min': 'tPF 10-Cycle Min', 
    'tPF_10_Cycle_Avg': 'tPF 10-Cycle Avg', 
    'tPF_10_Cycle_Max': 'tPF 10-Cycle Max', 
    'Carbon_Rate_10_Cycle_Min_kg_h': 'Carbon Rate 10-Cycle Min(kg/h)', 
    'Carbon_Rate_10_Cycle_Avg_kg_h': 'Carbon Rate 10-Cycle Avg(kg/h)', 
    'Carbon_Rate_10_Cycle_Max_kg_h': 'Carbon Rate 10-Cycle Max(kg/h)', 
    'VA_L1_10_Cycle_Min_kVA': 'VA L1 10-Cycle Min(kVA)', 
    'VA_L1_10_Cycle_Avg_kVA': 'VA L1 10-Cycle Avg(kVA)', 
    'VA_L1_10_Cycle_Max_kVA': 'VA L1 10-Cycle Max(kVA)', 
    'VA_L2_10_Cycle_Min_kVA': 'VA L2 10-Cycle Min(kVA)', 
    'VA_L2_10_Cycle_Avg_kVA': 'VA L2 10-Cycle Avg(kVA)', 
    'VA_L2_10_Cycle_Max_kVA': 'VA L2 10-Cycle Max(kVA)', 
    'VA_L3_10_Cycle_Min_kVA': 'VA L3 10-Cycle Min(kVA)', 
    'VA_L3_10_Cycle_Avg_kVA': 'VA L3 10-Cycle Avg(kVA)', 
    'VA_L3_10_Cycle_Max_kVA': 'VA L3 10-Cycle Max(kVA)', 
    'VAR_L1_10_Cycle_Min_kVAR': 'VAR L1 10-Cycle Min(kVAR)', 
    'VAR_L1_10_Cycle_Avg_kVAR': 'VAR L1 10-Cycle Avg(kVAR)', 
    'VAR_L1_10_Cycle_Max_kVAR': 'VAR L1 10-Cycle Max(kVAR)', 
    'VAR_L2_10_Cycle_Min_kVAR': 'VAR L2 10-Cycle Min(kVAR)', 
    'VAR_L2_10_Cycle_Avg_kVAR': 'VAR L2 10-Cycle Avg(kVAR)', 
    'VAR_L2_10_Cycle_Max_kVAR': 'VAR L2 10-Cycle Max(kVAR)', 
    'VAR_L3_10_Cycle_Min_kVAR': 'VAR L3 10-Cycle Min(kVAR)', 
    'VAR_L3_10_Cycle_Avg_kVAR': 'VAR L3 10-Cycle Avg(kVAR)', 
    'VAR_L3_10_Cycle_Max_kVAR': 'VAR L3 10-Cycle Max(kVAR)', 
    'Real_Power_L1_10_Cycle_Min_kW': 'Real Power L1 10-Cycle Min(kW)', 
    'Real_Power_L1_10_Cycle_Avg_kW': 'Real Power L1 10-Cycle Avg(kW)', 
    'Real_Power_L1_10_Cycle_Max_kW': 'Real Power L1 10-Cycle Max(kW)', 
    'Real_Power_L2_10_Cycle_Min_kW': 'Real Power L2 10-Cycle Min(kW)', 
    'Real_Power_L2_10_Cycle_Avg_kW': 'Real Power L2 10-Cycle Avg(kW)', 
    'Real_Power_L2_10_Cycle_Max_kW': 'Real Power L2 10-Cycle Max(kW)', 
    'Real_Power_L3_10_Cycle_Min_kW': 'Real Power L3 10-Cycle Min(kW)', 
    'Real_Power_L3_10_Cycle_Avg_kW': 'Real Power L3 10-Cycle Avg(kW)', 
    'Real_Power_L3_10_Cycle_Max_kW': 'Real Power L3 10-Cycle Max(kW)', 
    'tPF_L1_10_Cycle_Min': 'tPF L1 10-Cycle Min', 
    'tPF_L1_10_Cycle_Avg': 'tPF L1 10-Cycle Avg', 
    'tPF_L1_10_Cycle_Max': 'tPF L1 10-Cycle Max', 
    'tPF_L2_10_Cycle_Min': 'tPF L2 10-Cycle Min', 
    'tPF_L2_10_Cycle_Avg': 'tPF L2 10-Cycle Avg', 
    'tPF_L2_10_Cycle_Max': 'tPF L2 10-Cycle Max', 
    'tPF_L3_10_Cycle_Min': 'tPF L3 10-Cycle Min', 
    'tPF_L3_10_Cycle_Avg': 'tPF L3 10-Cycle Avg', 
    'tPF_L3_10_Cycle_Max': 'tPF L3 10-Cycle Max', 
    'Energy__kWH': 'Energy (kWH)'
}


CSV_labels_voltage_harmonics = {
    'H0_mag': 'H0 Harmonic Amplitude (V)', 
    'H0_ang': 'H0 Harmonic Angle (deg)', 
    'H0_inter_mag': 'H0 Interharmonic (V)', 
    'H1_mag': 'H1 Harmonic Amplitude (V)', 
    'H1_ang': 'H1 Harmonic Angle (deg)', 
    'H1_inter_mag': 'H1 Interharmonic (V)', 
    'H2_mag': 'H2 Harmonic Amplitude (V)', 
    'H2_ang': 'H2 Harmonic Angle (deg)', 
    'H2_inter_mag': 'H2 Interharmonic (V)', 
    'H3_mag': 'H3 Harmonic Amplitude (V)', 
    'H3_ang': 'H3 Harmonic Angle (deg)', 
    'H3_inter_mag': 'H3 Interharmonic (V)', 
    'H4_mag': 'H4 Harmonic Amplitude (V)', 
    'H4_ang': 'H4 Harmonic Angle (deg)', 
    'H4_inter_mag': 'H4 Interharmonic (V)', 
    'H5_mag': 'H5 Harmonic Amplitude (V)', 
    'H5_ang': 'H5 Harmonic Angle (deg)', 
    'H5_inter_mag': 'H5 Interharmonic (V)', 
    'H6_mag': 'H6 Harmonic Amplitude (V)', 
    'H6_ang': 'H6 Harmonic Angle (deg)', 
    'H6_inter_mag': 'H6 Interharmonic (V)', 
    'H7_mag': 'H7 Harmonic Amplitude (V)', 
    'H7_ang': 'H7 Harmonic Angle (deg)', 
    'H7_inter_mag': 'H7 Interharmonic (V)', 
    'H8_mag': 'H8 Harmonic Amplitude (V)', 
    'H8_ang': 'H8 Harmonic Angle (deg)', 
    'H8_inter_mag': 'H8 Interharmonic (V)', 
    'H9_mag': 'H9 Harmonic Amplitude (V)', 
    'H9_ang': 'H9 Harmonic Angle (deg)', 
    'H9_inter_mag': 'H9 Interharmonic (V)', 
    'H10_mag': 'H10 Harmonic Amplitude (V)', 
    'H10_ang': 'H10 Harmonic Angle (deg)', 
    'H10_inter_mag': 'H10 Interharmonic (V)', 
    'H11_mag': 'H11 Harmonic Amplitude (V)', 
    'H11_ang': 'H11 Harmonic Angle (deg)', 
    'H11_inter_mag': 'H11 Interharmonic (V)', 
    'H12_mag': 'H12 Harmonic Amplitude (V)', 
    'H12_ang': 'H12 Harmonic Angle (deg)', 
    'H12_inter_mag': 'H12 Interharmonic (V)', 
    'H13_mag': 'H13 Harmonic Amplitude (V)', 
    'H13_ang': 'H13 Harmonic Angle (deg)', 
    'H13_inter_mag': 'H13 Interharmonic (V)', 
    'H14_mag': 'H14 Harmonic Amplitude (V)', 
    'H14_ang': 'H14 Harmonic Angle (deg)', 
    'H14_inter_mag': 'H14 Interharmonic (V)', 
    'H15_mag': 'H15 Harmonic Amplitude (V)', 
    'H15_ang': 'H15 Harmonic Angle (deg)', 
    'H15_inter_mag': 'H15 Interharmonic (V)', 
    'H16_mag': 'H16 Harmonic Amplitude (V)', 
    'H16_ang': 'H16 Harmonic Angle (deg)', 
    'H16_inter_mag': 'H16 Interharmonic (V)', 
    'H17_mag': 'H17 Harmonic Amplitude (V)', 
    'H17_ang': 'H17 Harmonic Angle (deg)', 
    'H17_inter_mag': 'H17 Interharmonic (V)', 
    'H18_mag': 'H18 Harmonic Amplitude (V)', 
    'H18_ang': 'H18 Harmonic Angle (deg)', 
    'H18_inter_mag': 'H18 Interharmonic (V)', 
    'H19_mag': 'H19 Harmonic Amplitude (V)', 
    'H19_ang': 'H19 Harmonic Angle (deg)', 
    'H19_inter_mag': 'H19 Interharmonic (V)', 
    'H20_mag': 'H20 Harmonic Amplitude (V)', 
    'H20_ang': 'H20 Harmonic Angle (deg)', 
    'H20_inter_mag': 'H20 Interharmonic (V)', 
    'H21_mag': 'H21 Harmonic Amplitude (V)', 
    'H21_ang': 'H21 Harmonic Angle (deg)', 
    'H21_inter_mag': 'H21 Interharmonic (V)', 
    'H22_mag': 'H22 Harmonic Amplitude (V)', 
    'H22_ang': 'H22 Harmonic Angle (deg)', 
    'H22_inter_mag': 'H22 Interharmonic (V)', 
    'H23_mag': 'H23 Harmonic Amplitude (V)', 
    'H23_ang': 'H23 Harmonic Angle (deg)', 
    'H23_inter_mag': 'H23 Interharmonic (V)', 
    'H24_mag': 'H24 Harmonic Amplitude (V)', 
    'H24_ang': 'H24 Harmonic Angle (deg)', 
    'H24_inter_mag': 'H24 Interharmonic (V)', 
    'H25_mag': 'H25 Harmonic Amplitude (V)', 
    'H25_ang': 'H25 Harmonic Angle (deg)', 
    'H25_inter_mag': 'H25 Interharmonic (V)', 
    'H26_mag': 'H26 Harmonic Amplitude (V)', 
    'H26_ang': 'H26 Harmonic Angle (deg)', 
    'H26_inter_mag': 'H26 Interharmonic (V)', 
    'H27_mag': 'H27 Harmonic Amplitude (V)', 
    'H27_ang': 'H27 Harmonic Angle (deg)', 
    'H27_inter_mag': 'H27 Interharmonic (V)', 
    'H28_mag': 'H28 Harmonic Amplitude (V)', 
    'H28_ang': 'H28 Harmonic Angle (deg)', 
    'H28_inter_mag': 'H28 Interharmonic (V)', 
    'H29_mag': 'H29 Harmonic Amplitude (V)', 
    'H29_ang': 'H29 Harmonic Angle (deg)', 
    'H29_inter_mag': 'H29 Interharmonic (V)', 
    'H30_mag': 'H30 Harmonic Amplitude (V)', 
    'H30_ang': 'H30 Harmonic Angle (deg)', 
    'H30_inter_mag': 'H30 Interharmonic (V)', 
    'H31_mag': 'H31 Harmonic Amplitude (V)', 
    'H31_ang': 'H31 Harmonic Angle (deg)', 
    'H31_inter_mag': 'H31 Interharmonic (V)', 
    'H32_mag': 'H32 Harmonic Amplitude (V)', 
    'H32_ang': 'H32 Harmonic Angle (deg)', 
    'H32_inter_mag': 'H32 Interharmonic (V)', 
    'H33_mag': 'H33 Harmonic Amplitude (V)', 
    'H33_ang': 'H33 Harmonic Angle (deg)', 
    'H33_inter_mag': 'H33 Interharmonic (V)', 
    'H34_mag': 'H34 Harmonic Amplitude (V)', 
    'H34_ang': 'H34 Harmonic Angle (deg)', 
    'H34_inter_mag': 'H34 Interharmonic (V)', 
    'H35_mag': 'H35 Harmonic Amplitude (V)', 
    'H35_ang': 'H35 Harmonic Angle (deg)', 
    'H35_inter_mag': 'H35 Interharmonic (V)', 
    'H36_mag': 'H36 Harmonic Amplitude (V)', 
    'H36_ang': 'H36 Harmonic Angle (deg)', 
    'H36_inter_mag': 'H36 Interharmonic (V)', 
    'H37_mag': 'H37 Harmonic Amplitude (V)', 
    'H37_ang': 'H37 Harmonic Angle (deg)', 
    'H37_inter_mag': 'H37 Interharmonic (V)', 
    'H38_mag': 'H38 Harmonic Amplitude (V)', 
    'H38_ang': 'H38 Harmonic Angle (deg)', 
    'H38_inter_mag': 'H38 Interharmonic (V)', 
    'H39_mag': 'H39 Harmonic Amplitude (V)', 
    'H39_ang': 'H39 Harmonic Angle (deg)', 
    'H39_inter_mag': 'H39 Interharmonic (V)', 
    'H40_mag': 'H40 Harmonic Amplitude (V)', 
    'H40_ang': 'H40 Harmonic Angle (deg)', 
    'H40_inter_mag': 'H40 Interharmonic (V)', 
    'H41_mag': 'H41 Harmonic Amplitude (V)', 
    'H41_ang': 'H41 Harmonic Angle (deg)', 
    'H41_inter_mag': 'H41 Interharmonic (V)', 
    'H42_mag': 'H42 Harmonic Amplitude (V)', 
    'H42_ang': 'H42 Harmonic Angle (deg)', 
    'H42_inter_mag': 'H42 Interharmonic (V)', 
    'H43_mag': 'H43 Harmonic Amplitude (V)', 
    'H43_ang': 'H43 Harmonic Angle (deg)', 
    'H43_inter_mag': 'H43 Interharmonic (V)', 
    'H44_mag': 'H44 Harmonic Amplitude (V)', 
    'H44_ang': 'H44 Harmonic Angle (deg)', 
    'H44_inter_mag': 'H44 Interharmonic (V)', 
    'H45_mag': 'H45 Harmonic Amplitude (V)', 
    'H45_ang': 'H45 Harmonic Angle (deg)', 
    'H45_inter_mag': 'H45 Interharmonic (V)', 
    'H46_mag': 'H46 Harmonic Amplitude (V)', 
    'H46_ang': 'H46 Harmonic Angle (deg)', 
    'H46_inter_mag': 'H46 Interharmonic (V)', 
    'H47_mag': 'H47 Harmonic Amplitude (V)', 
    'H47_ang': 'H47 Harmonic Angle (deg)', 
    'H47_inter_mag': 'H47 Interharmonic (V)', 
    'H48_mag': 'H48 Harmonic Amplitude (V)', 
    'H48_ang': 'H48 Harmonic Angle (deg)', 
    'H48_inter_mag': 'H48 Interharmonic (V)', 
    'H49_mag': 'H49 Harmonic Amplitude (V)', 
    'H49_ang': 'H49 Harmonic Angle (deg)', 
    'H49_inter_mag': 'H49 Interharmonic (V)', 
    'H50_mag': 'H50 Harmonic Amplitude (V)', 
    'H50_ang': 'H50 Harmonic Angle (deg)', 
    'H50_inter_mag': 'H50 Interharmonic (V)', 
    'H51_mag': 'H51 Harmonic Amplitude (V)', 
    'H51_ang': 'H51 Harmonic Angle (deg)', 
    'H51_inter_mag': 'H51 Interharmonic (V)', 
    'H52_mag': 'H52 Harmonic Amplitude (V)', 
    'H52_ang': 'H52 Harmonic Angle (deg)', 
    'H52_inter_mag': 'H52 Interharmonic (V)', 
    'H53_mag': 'H53 Harmonic Amplitude (V)', 
    'H53_ang': 'H53 Harmonic Angle (deg)', 
    'H53_inter_mag': 'H53 Interharmonic (V)', 
    'H54_mag': 'H54 Harmonic Amplitude (V)', 
    'H54_ang': 'H54 Harmonic Angle (deg)', 
    'H54_inter_mag': 'H54 Interharmonic (V)', 
    'H55_mag': 'H55 Harmonic Amplitude (V)', 
    'H55_ang': 'H55 Harmonic Angle (deg)', 
    'H55_inter_mag': 'H55 Interharmonic (V)', 
    'H56_mag': 'H56 Harmonic Amplitude (V)', 
    'H56_ang': 'H56 Harmonic Angle (deg)', 
    'H56_inter_mag': 'H56 Interharmonic (V)', 
    'H57_mag': 'H57 Harmonic Amplitude (V)', 
    'H57_ang': 'H57 Harmonic Angle (deg)', 
    'H57_inter_mag': 'H57 Interharmonic (V)', 
    'H58_mag': 'H58 Harmonic Amplitude (V)', 
    'H58_ang': 'H58 Harmonic Angle (deg)', 
    'H58_inter_mag': 'H58 Interharmonic (V)', 
    'H59_mag': 'H59 Harmonic Amplitude (V)', 
    'H59_ang': 'H59 Harmonic Angle (deg)', 
    'H59_inter_mag': 'H59 Interharmonic (V)', 
    'H60_mag': 'H60 Harmonic Amplitude (V)', 
    'H60_ang': 'H60 Harmonic Angle (deg)', 
    'H60_inter_mag': 'H60 Interharmonic (V)', 
    'H61_mag': 'H61 Harmonic Amplitude (V)', 
    'H61_ang': 'H61 Harmonic Angle (deg)', 
    'H61_inter_mag': 'H61 Interharmonic (V)', 
    'H62_mag': 'H62 Harmonic Amplitude (V)', 
    'H62_ang': 'H62 Harmonic Angle (deg)', 
    'H62_inter_mag': 'H62 Interharmonic (V)', 
    'H63_mag': 'H63 Harmonic Amplitude (V)', 
    'H63_ang': 'H63 Harmonic Angle (deg)', 
    'H63_inter_mag': 'H63 Interharmonic (V)'
}

CSV_labels_current_harmonics = {
    'H0_mag': 'H0 Harmonic Amplitude (A)', 
    'H0_ang': 'H0 Harmonic Angle (deg)', 
    'H0_inter_mag': 'H0 Interharmonic (A)', 
    'H1_mag': 'H1 Harmonic Amplitude (A)', 
    'H1_ang': 'H1 Harmonic Angle (deg)', 
    'H1_inter_mag': 'H1 Interharmonic (A)', 
    'H2_mag': 'H2 Harmonic Amplitude (A)', 
    'H2_ang': 'H2 Harmonic Angle (deg)', 
    'H2_inter_mag': 'H2 Interharmonic (A)', 
    'H3_mag': 'H3 Harmonic Amplitude (A)', 
    'H3_ang': 'H3 Harmonic Angle (deg)', 
    'H3_inter_mag': 'H3 Interharmonic (A)', 
    'H4_mag': 'H4 Harmonic Amplitude (A)', 
    'H4_ang': 'H4 Harmonic Angle (deg)', 
    'H4_inter_mag': 'H4 Interharmonic (A)', 
    'H5_mag': 'H5 Harmonic Amplitude (A)', 
    'H5_ang': 'H5 Harmonic Angle (deg)', 
    'H5_inter_mag': 'H5 Interharmonic (A)', 
    'H6_mag': 'H6 Harmonic Amplitude (A)', 
    'H6_ang': 'H6 Harmonic Angle (deg)', 
    'H6_inter_mag': 'H6 Interharmonic (A)', 
    'H7_mag': 'H7 Harmonic Amplitude (A)', 
    'H7_ang': 'H7 Harmonic Angle (deg)', 
    'H7_inter_mag': 'H7 Interharmonic (A)', 
    'H8_mag': 'H8 Harmonic Amplitude (A)', 
    'H8_ang': 'H8 Harmonic Angle (deg)', 
    'H8_inter_mag': 'H8 Interharmonic (A)', 
    'H9_mag': 'H9 Harmonic Amplitude (A)', 
    'H9_ang': 'H9 Harmonic Angle (deg)', 
    'H9_inter_mag': 'H9 Interharmonic (A)', 
    'H10_mag': 'H10 Harmonic Amplitude (A)', 
    'H10_ang': 'H10 Harmonic Angle (deg)', 
    'H10_inter_mag': 'H10 Interharmonic (A)', 
    'H11_mag': 'H11 Harmonic Amplitude (A)', 
    'H11_ang': 'H11 Harmonic Angle (deg)', 
    'H11_inter_mag': 'H11 Interharmonic (A)', 
    'H12_mag': 'H12 Harmonic Amplitude (A)', 
    'H12_ang': 'H12 Harmonic Angle (deg)', 
    'H12_inter_mag': 'H12 Interharmonic (A)', 
    'H13_mag': 'H13 Harmonic Amplitude (A)', 
    'H13_ang': 'H13 Harmonic Angle (deg)', 
    'H13_inter_mag': 'H13 Interharmonic (A)', 
    'H14_mag': 'H14 Harmonic Amplitude (A)', 
    'H14_ang': 'H14 Harmonic Angle (deg)', 
    'H14_inter_mag': 'H14 Interharmonic (A)', 
    'H15_mag': 'H15 Harmonic Amplitude (A)', 
    'H15_ang': 'H15 Harmonic Angle (deg)', 
    'H15_inter_mag': 'H15 Interharmonic (A)', 
    'H16_mag': 'H16 Harmonic Amplitude (A)', 
    'H16_ang': 'H16 Harmonic Angle (deg)', 
    'H16_inter_mag': 'H16 Interharmonic (A)', 
    'H17_mag': 'H17 Harmonic Amplitude (A)', 
    'H17_ang': 'H17 Harmonic Angle (deg)', 
    'H17_inter_mag': 'H17 Interharmonic (A)', 
    'H18_mag': 'H18 Harmonic Amplitude (A)', 
    'H18_ang': 'H18 Harmonic Angle (deg)', 
    'H18_inter_mag': 'H18 Interharmonic (A)', 
    'H19_mag': 'H19 Harmonic Amplitude (A)', 
    'H19_ang': 'H19 Harmonic Angle (deg)', 
    'H19_inter_mag': 'H19 Interharmonic (A)', 
    'H20_mag': 'H20 Harmonic Amplitude (A)', 
    'H20_ang': 'H20 Harmonic Angle (deg)', 
    'H20_inter_mag': 'H20 Interharmonic (A)', 
    'H21_mag': 'H21 Harmonic Amplitude (A)', 
    'H21_ang': 'H21 Harmonic Angle (deg)', 
    'H21_inter_mag': 'H21 Interharmonic (A)', 
    'H22_mag': 'H22 Harmonic Amplitude (A)', 
    'H22_ang': 'H22 Harmonic Angle (deg)', 
    'H22_inter_mag': 'H22 Interharmonic (A)', 
    'H23_mag': 'H23 Harmonic Amplitude (A)', 
    'H23_ang': 'H23 Harmonic Angle (deg)', 
    'H23_inter_mag': 'H23 Interharmonic (A)', 
    'H24_mag': 'H24 Harmonic Amplitude (A)', 
    'H24_ang': 'H24 Harmonic Angle (deg)', 
    'H24_inter_mag': 'H24 Interharmonic (A)', 
    'H25_mag': 'H25 Harmonic Amplitude (A)', 
    'H25_ang': 'H25 Harmonic Angle (deg)', 
    'H25_inter_mag': 'H25 Interharmonic (A)', 
    'H26_mag': 'H26 Harmonic Amplitude (A)', 
    'H26_ang': 'H26 Harmonic Angle (deg)', 
    'H26_inter_mag': 'H26 Interharmonic (A)', 
    'H27_mag': 'H27 Harmonic Amplitude (A)', 
    'H27_ang': 'H27 Harmonic Angle (deg)', 
    'H27_inter_mag': 'H27 Interharmonic (A)', 
    'H28_mag': 'H28 Harmonic Amplitude (A)', 
    'H28_ang': 'H28 Harmonic Angle (deg)', 
    'H28_inter_mag': 'H28 Interharmonic (A)', 
    'H29_mag': 'H29 Harmonic Amplitude (A)', 
    'H29_ang': 'H29 Harmonic Angle (deg)', 
    'H29_inter_mag': 'H29 Interharmonic (A)', 
    'H30_mag': 'H30 Harmonic Amplitude (A)', 
    'H30_ang': 'H30 Harmonic Angle (deg)', 
    'H30_inter_mag': 'H30 Interharmonic (A)', 
    'H31_mag': 'H31 Harmonic Amplitude (A)', 
    'H31_ang': 'H31 Harmonic Angle (deg)', 
    'H31_inter_mag': 'H31 Interharmonic (A)', 
    'H32_mag': 'H32 Harmonic Amplitude (A)', 
    'H32_ang': 'H32 Harmonic Angle (deg)', 
    'H32_inter_mag': 'H32 Interharmonic (A)', 
    'H33_mag': 'H33 Harmonic Amplitude (A)', 
    'H33_ang': 'H33 Harmonic Angle (deg)', 
    'H33_inter_mag': 'H33 Interharmonic (A)', 
    'H34_mag': 'H34 Harmonic Amplitude (A)', 
    'H34_ang': 'H34 Harmonic Angle (deg)', 
    'H34_inter_mag': 'H34 Interharmonic (A)', 
    'H35_mag': 'H35 Harmonic Amplitude (A)', 
    'H35_ang': 'H35 Harmonic Angle (deg)', 
    'H35_inter_mag': 'H35 Interharmonic (A)', 
    'H36_mag': 'H36 Harmonic Amplitude (A)', 
    'H36_ang': 'H36 Harmonic Angle (deg)', 
    'H36_inter_mag': 'H36 Interharmonic (A)', 
    'H37_mag': 'H37 Harmonic Amplitude (A)', 
    'H37_ang': 'H37 Harmonic Angle (deg)', 
    'H37_inter_mag': 'H37 Interharmonic (A)', 
    'H38_mag': 'H38 Harmonic Amplitude (A)', 
    'H38_ang': 'H38 Harmonic Angle (deg)', 
    'H38_inter_mag': 'H38 Interharmonic (A)', 
    'H39_mag': 'H39 Harmonic Amplitude (A)', 
    'H39_ang': 'H39 Harmonic Angle (deg)', 
    'H39_inter_mag': 'H39 Interharmonic (A)', 
    'H40_mag': 'H40 Harmonic Amplitude (A)', 
    'H40_ang': 'H40 Harmonic Angle (deg)', 
    'H40_inter_mag': 'H40 Interharmonic (A)', 
    'H41_mag': 'H41 Harmonic Amplitude (A)', 
    'H41_ang': 'H41 Harmonic Angle (deg)', 
    'H41_inter_mag': 'H41 Interharmonic (A)', 
    'H42_mag': 'H42 Harmonic Amplitude (A)', 
    'H42_ang': 'H42 Harmonic Angle (deg)', 
    'H42_inter_mag': 'H42 Interharmonic (A)', 
    'H43_mag': 'H43 Harmonic Amplitude (A)', 
    'H43_ang': 'H43 Harmonic Angle (deg)', 
    'H43_inter_mag': 'H43 Interharmonic (A)', 
    'H44_mag': 'H44 Harmonic Amplitude (A)', 
    'H44_ang': 'H44 Harmonic Angle (deg)', 
    'H44_inter_mag': 'H44 Interharmonic (A)', 
    'H45_mag': 'H45 Harmonic Amplitude (A)', 
    'H45_ang': 'H45 Harmonic Angle (deg)', 
    'H45_inter_mag': 'H45 Interharmonic (A)', 
    'H46_mag': 'H46 Harmonic Amplitude (A)', 
    'H46_ang': 'H46 Harmonic Angle (deg)', 
    'H46_inter_mag': 'H46 Interharmonic (A)', 
    'H47_mag': 'H47 Harmonic Amplitude (A)', 
    'H47_ang': 'H47 Harmonic Angle (deg)', 
    'H47_inter_mag': 'H47 Interharmonic (A)', 
    'H48_mag': 'H48 Harmonic Amplitude (A)', 
    'H48_ang': 'H48 Harmonic Angle (deg)', 
    'H48_inter_mag': 'H48 Interharmonic (A)', 
    'H49_mag': 'H49 Harmonic Amplitude (A)', 
    'H49_ang': 'H49 Harmonic Angle (deg)', 
    'H49_inter_mag': 'H49 Interharmonic (A)', 
    'H50_mag': 'H50 Harmonic Amplitude (A)', 
    'H50_ang': 'H50 Harmonic Angle (deg)', 
    'H50_inter_mag': 'H50 Interharmonic (A)', 
    'H51_mag': 'H51 Harmonic Amplitude (A)', 
    'H51_ang': 'H51 Harmonic Angle (deg)', 
    'H51_inter_mag': 'H51 Interharmonic (A)', 
    'H52_mag': 'H52 Harmonic Amplitude (A)', 
    'H52_ang': 'H52 Harmonic Angle (deg)', 
    'H52_inter_mag': 'H52 Interharmonic (A)', 
    'H53_mag': 'H53 Harmonic Amplitude (A)', 
    'H53_ang': 'H53 Harmonic Angle (deg)', 
    'H53_inter_mag': 'H53 Interharmonic (A)', 
    'H54_mag': 'H54 Harmonic Amplitude (A)', 
    'H54_ang': 'H54 Harmonic Angle (deg)', 
    'H54_inter_mag': 'H54 Interharmonic (A)', 
    'H55_mag': 'H55 Harmonic Amplitude (A)', 
    'H55_ang': 'H55 Harmonic Angle (deg)', 
    'H55_inter_mag': 'H55 Interharmonic (A)', 
    'H56_mag': 'H56 Harmonic Amplitude (A)', 
    'H56_ang': 'H56 Harmonic Angle (deg)', 
    'H56_inter_mag': 'H56 Interharmonic (A)', 
    'H57_mag': 'H57 Harmonic Amplitude (A)', 
    'H57_ang': 'H57 Harmonic Angle (deg)', 
    'H57_inter_mag': 'H57 Interharmonic (A)', 
    'H58_mag': 'H58 Harmonic Amplitude (A)', 
    'H58_ang': 'H58 Harmonic Angle (deg)', 
    'H58_inter_mag': 'H58 Interharmonic (A)', 
    'H59_mag': 'H59 Harmonic Amplitude (A)', 
    'H59_ang': 'H59 Harmonic Angle (deg)', 
    'H59_inter_mag': 'H59 Interharmonic (A)', 
    'H60_mag': 'H60 Harmonic Amplitude (A)', 
    'H60_ang': 'H60 Harmonic Angle (deg)', 
    'H60_inter_mag': 'H60 Interharmonic (A)', 
    'H61_mag': 'H61 Harmonic Amplitude (A)', 
    'H61_ang': 'H61 Harmonic Angle (deg)', 
    'H61_inter_mag': 'H61 Interharmonic (A)', 
    'H62_mag': 'H62 Harmonic Amplitude (A)', 
    'H62_ang': 'H62 Harmonic Angle (deg)', 
    'H62_inter_mag': 'H62 Interharmonic (A)', 
    'H63_mag': 'H63 Harmonic Amplitude (A)', 
    'H63_ang': 'H63 Harmonic Angle (deg)', 
    'H63_inter_mag': 'H63 Interharmonic (A)'
}

CSV_labels_events = {
    'Milliseconds': 'Milliseconds',
    'N_E__V': 'N-E (V)',
    'L1_N__V': 'L1-N (V)',
    'L2_N__V': 'L2-N (V)',
    'L3_N__V': 'L3-N (V)',
    'L1_Amp__A': 'L1 Amp (A)',
    'L2_Amp__A': 'L2 Amp (A)',
    'L3_Amp__A': 'L3 Amp (A)',
    'N_Amp__A': 'N Amp (A)'
}

# FILTER = Filters(complib='blosc:lz4', complevel=1)    # enable LZ4 compression algorithm
FILTER = None
TREND_DIRECTORY_THRESHOLD_MB = 1.0                  # size threshold for day data directories, in megabytes
DAY_DIRECTORY_THRESHOLD_MB = 20.0                  # max size threshold for day data directories, in megabytes
MONITORING_DATA_FILENAME = 'monitoring-data'
HARMONICS_DATA_FILENAMES = [
    'L1-N(V) Harmonics',
    'L2-N(V) Harmonics',
    'L3-N(V) Harmonics',
    'L1(A) Harmonics',
    'L2(A) Harmonics',
    'L3(A) Harmonics'
]
EVENTS_DATA_FILENAME = 'event-data'


def get_data_from_CSV_label(label, CSV_row):
    if label in CSV_row:
        try:
            return float(CSV_row[label])
        except ValueError, e:
            pass
    return 0.0

def get_data_from_CSV_label_harmonics(label, CSV_row):
    if label in CSV_row:
        try:
            ret = float(CSV_row[label])
            # convert angles from degrees to radians
            if 'Angle' in label:
                ret = np.deg2rad(ret)
            return ret
        except ValueError, e:
            pass
    return 0.0

# recursively find the size of a directory
def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def title_except(s, exceptions):
    word_list = re.split(' ', s)
    final = [word_list[0].capitalize()]
    for word in word_list[1:]:
        final.append(word in exceptions and word or word.capitalize())
    return ' '.join(final)

def get_secondary_ID(secondary_name):
    m = re.search(r"\((\w+)\)", secondary_name)
    if m is not None and m.group(1) is not None:
        return m.group(1)
    
    return 0

def formatted_substation_name(substation_name):
    new_substation_name = substation_name

    # remove brackets and contents
    start = new_substation_name.find(' (')
    end = new_substation_name.find(')')
    if start != -1 and end != -1:
      new_substation_name = new_substation_name[0:start]

    # change case
    new_substation_name = title_except(new_substation_name.lower(), ['on', 'a', 'an', 'of', 'the', 'is'])

    # replace abbreviations
    return new_substation_name                      \
        .replace(' St', ' Street')                  \
        .replace(' Streetreet', ' Street')          \
        .replace(' Ln', ' Lane')                    \
        .replace(' Rd N', ' Road North')            \
        .replace(' Rd', ' Road')                    \
        .replace(' Ave', ' Avenue')                 \
        .replace(' Dr', ' Drive')                   \
        .replace(' Pk', ' Park')                    \
        .replace(' Bk', ' Back')                    \
        .replace(' Fm', ' Farm')                    \
        .replace(' Cres', ' Crescent')              \
        .replace(' Cl', ' Close')                   \
        .replace(' Est', ' Estate')                 \
        .replace(' Rec', ' Recreation Ground')      \
        .replace(' Sec', ' Secondary')              \
        .replace(' Sch', ' School')                 \
        .replace(' Schoolool', ' School')           \
        .replace(' Stn', ' Station')                \
        .replace(' Hsg', ' Housing')                \
        .replace(' Hs', ' House')                   \
        .replace(' Macc', ' Macclesfield')          \
        .replace(' Bk ', ' Back ')                  \
        .replace(' Estateate', ' Estate')           \
        .replace(' Oneills', ' O\'Neills')          \
        .replace(' Streetation', ' Station')        \
        .replace('Bk George', 'Back George')


def import_waveform(filenames, path, data_row, batch_number):
    start_time = 0.0
    end_time = 0.0
    temp_data = {}
    for item in CSV_labels_events:
        temp_data[item] = []

    for i, filename in enumerate(filenames):
        path_event_csv = path + os.sep + filename
        with open(path_event_csv, 'rb') as csvfile:
            try:
                while True:
                    line = csvfile.readline().replace('"', '').replace('\n', '').replace('\r', '')
                    if line is None:
                        raise Exception('line is None')

                    if 'Event_Type' in line:
                        Event_Type = line.split(',')[1]
                    elif 'Event_Magnitude' in line:
                        Event_Magnitude = line.split(',')[1]
                    elif 'Event_Duration_In_Seconds' in line:
                        Event_Duration_In_Seconds = line.split(',')[1]
                    elif 'Trigger_Date' in line:
                        Trigger_Date = line.split(',')[1]
                    elif 'Trigger_Time' in line:
                        Trigger_Time = line.split(',')[1]
                    elif 'Trigger_Channel' in line:
                        Trigger_Channel = line.split(',')[1]
                    elif 'Trigger_Sample_Number' in line:
                        Trigger_Sample_Number = line.split(',')[1]
                    elif 'Samples_Per_Cycle' in line:
                        Samples_Per_Cycle = line.split(',')[1]
                    elif 'Microseconds_Per_Sample' in line:
                        Microseconds_Per_Sample = line.split(',')[1]
                        next(csvfile)
                        # line = csvfile.readline().replace('"', '').replace('\n', '').replace('\r', '')
                        break

                # print Event_Type
                # print Trigger_Date
                # print Trigger_Time
                # if 'Trigger_Channel' in locals():
                #     print Trigger_Channel
                # print Trigger_Sample_Number
                # if 'Samples_Per_Cycle' not in locals():
                #     print 'missing Samples_Per_Cycle', path_event_csv
                # else:
                #     print Samples_Per_Cycle
                # print Microseconds_Per_Sample

                full_date_str = Trigger_Date + ' ' + Trigger_Time.replace(' UTC', '').replace('T ', '')
                try:
                    full_date = datetime.datetime.strptime(full_date_str, '%Y/%m/%d %H:%M:%S.%f')
                except Exception:
                    full_date = datetime.datetime.strptime(full_date_str, '%d/%m/%Y %H:%M:%S.%f')
                event_timestamp = calendar.timegm(full_date.timetuple())

                # create a DictReader to index the data in the CSV file
                reader = csv.DictReader(csvfile)
                for r, row in enumerate(reader):
                    for item in CSV_labels_events:
                        temp_data[item].append(get_data_from_CSV_label(CSV_labels_events[item], row))
                        if r == 0 and item == 'Milliseconds':
                            start_time = temp_data[item][-1]
                        elif r == 2047 and item == 'Milliseconds':
                            end_time = temp_data[item][-1]


                if i == len(filenames) - 1:
                    data_row['date'] = event_timestamp
                    data_row['batch'] = batch_number

                    if len(filenames) == 2:
                        data_row['two_part'] = True
                    else:
                        data_row['two_part'] = False

                    data_row['Event_Type'] = Event_Type
                    if 'Trigger_Channel' in locals():
                        data_row['Trigger_Channel'] = Trigger_Channel
                    data_row['Trigger_Sample_Number'] = Trigger_Sample_Number
                    data_row['Samples_Per_Cycle'] = Samples_Per_Cycle
                    data_row['Microseconds_Per_Sample'] = Microseconds_Per_Sample

                    for item in CSV_labels_events:
                        if len(temp_data[item]) == 2048:
                            data_row[item] = temp_data[item]

                    if start_time is not None and end_time is not None:
                        # print start_time, end_time, abs(start_time) + abs(end_time)
                        data_row["duration_ms"] = abs(start_time) + abs(end_time)
                    data_row.append()
            except Exception as ex:
                print 'CSV exception'
                print path_event_csv
                print ex

def import_year(m, group, table, data_row, year, batch_number, monitoring_type='monitoring', harmonics_filename=''):
    # m['has_' + year] = True
    dirs_in_year = os.listdir(m['path'] + os.sep + year)

    if monitoring_type == 'harmonics':
        month_dirs = []
        for dir_name in dirs_in_year:
            if 'Month' in dir_name:
                month_dirs.append(dir_name)
        month_dirs.sort()

        for month_dir in month_dirs:
            path_month = m['path'] + os.sep + year + os.sep + month_dir
            dirs_month = os.listdir(path_month)
            print '  ' + month_dir

            day_dirs = []
            for dir_name in dirs_month:
                if 'Day' in dir_name:
                    day_dirs.append(dir_name)
            day_dirs.sort()

            for day_dir in day_dirs:
                path_day = m['path'] + os.sep + year + os.sep + month_dir + os.sep + day_dir
                dirs_day = os.listdir(path_day)

                for dir_name in dirs_day:
                    if ' Trends-Stats' in dir_name:
                        path_trends_dir = m['path'] + os.sep + year + os.sep + month_dir + os.sep + day_dir + os.sep + dir_name
                        dirs_trends = os.listdir(path_trends_dir)

                        if 'Spreadsheets' in dirs_trends:
                            path_spreadsheets = path_trends_dir + os.sep + 'Spreadsheets'
                            files = [f for f in os.listdir(path_spreadsheets) if os.path.isfile(os.path.join(path_spreadsheets, f))]

                            if harmonics_filename + '.csv' in files:
                                path_trends_csv = path_spreadsheets + os.sep + harmonics_filename + '.csv'
                                # print path_trends_csv
                                with open(path_trends_csv, 'rb') as csvfile:
                                    # ignore the first 9 lines
                                    try:
                                        for _ in xrange(9):
                                            next(csvfile)

                                        # create a DictReader to index the data in the CSV file
                                        reader = csv.DictReader(csvfile)
                                        for row in reader:
                                            csv_date = 'Time'

                                            if csv_date in row:
                                                try:
                                                    date = datetime.datetime.strptime(row[csv_date], '%Y/%m/%d %H:%M:%S')
                                                except ValueError:
                                                    print 'wrong format:', row[csv_date], path_trends_csv
                                                    date = datetime.datetime.strptime(row[csv_date], '%d/%m/%Y %H:%M')

                                                # date_tuple = (date.year, date.month, date.day, date.hour, date.minute)
                                                # time = datetime.time.strftime(row[csv_date], '%H %M %S')
                                                # print date
                                                if date < m['earliest_date']:
                                                    m['earliest_date'] = date
                                                if date > m['latest_date']:
                                                    m['latest_date'] = date

                                                # values = [row_existing['date'] for row_existing in table.where('(date == ' + str(calendar.timegm(date.timetuple())) + ')')]
                                                # if len(values) != 0:
                                                #     print 'date already exists'
                                                # else:
                                                data_row['date']  = calendar.timegm(date.timetuple())
                                                data_row['batch'] = batch_number

                                                if '(V)' in harmonics_filename:
                                                    for item in CSV_labels_voltage_harmonics:
                                                        data_row[item] = get_data_from_CSV_label_harmonics(CSV_labels_voltage_harmonics[item], row)
                                                else:
                                                    for item in CSV_labels_current_harmonics:
                                                        data_row[item] = get_data_from_CSV_label_harmonics(CSV_labels_current_harmonics[item], row)
                                                data_row.append()
                                    except Exception as ex:
                                        print 'CSV exception'
                                        print ex
        table.flush()
    elif monitoring_type == 'events':
        month_dirs = []
        for dir_name in dirs_in_year:
            if 'Month' in dir_name:
                month_dirs.append(dir_name)
        month_dirs.sort()

        for month_dir in month_dirs:
            path_month = m['path'] + os.sep + year + os.sep + month_dir
            dirs_month = os.listdir(path_month)
            print '  ' + month_dir

            day_dirs = []
            for dir_name in dirs_month:
                if 'Day' in dir_name:
                    day_dirs.append(dir_name)
            day_dirs.sort()

            for day_dir in day_dirs:
                path_day = m['path'] + os.sep + year + os.sep + month_dir + os.sep + day_dir
                dirs_day = os.listdir(path_day)

                size = get_size(start_path=path_day)
                number_of_phase_current_trigger = len([1 for dir_name in dirs_day if 'Phase Current Trigger' in dir_name])
                # print size / (1024 * 1024), number_of_phase_current_trigger

                if size < DAY_DIRECTORY_THRESHOLD_MB * 1024 * 1024 and number_of_phase_current_trigger < 6:
                    for dir_name in dirs_day:
                        if ' Trends-Stats' not in dir_name and 'Snapshot' not in dir_name:
                            path_event_dir = m['path'] + os.sep + year + os.sep + month_dir + os.sep + day_dir + os.sep + dir_name
                            dirs_event = os.listdir(path_event_dir)

                            if 'Spreadsheets' in dirs_event and 'Summaries' in dirs_event:
                                path_spreadsheets = path_event_dir + os.sep + 'Spreadsheets'
                                files_spreadsheets = [f for f in os.listdir(path_spreadsheets) if os.path.isfile(os.path.join(path_spreadsheets, f))]
                                
                                waveform_filename = None
                                beg_filename = None
                                end_filename = None
                                for filename in files_spreadsheets:
                                    if 'Waveform.csv' in filename:
                                        waveform_filename = filename
                                    if 'Beg.csv' in filename:
                                        beg_filename = filename
                                    if 'End.csv' in filename:
                                        end_filename = filename

                                if waveform_filename is not None:
                                    import_waveform([waveform_filename], path_spreadsheets, data_row, batch_number)
                                elif beg_filename is not None and end_filename is not None:
                                    import_waveform([beg_filename, end_filename], path_spreadsheets, data_row, batch_number)
                # else:
                #     print 'day size exceeded:', size / (1024 * 1024), number_of_phase_current_trigger
        table.flush()
    else:
        if year + ' Weekly Trends-Stats' in dirs_in_year:
            path_weekly_trends = m['path'] + os.sep + year + os.sep + year + ' Weekly Trends-Stats'
            dirs_weekly_trends = os.listdir(path_weekly_trends)
            for week in dirs_weekly_trends:
                print '  ' + week
                size = get_size(start_path=path_weekly_trends + os.sep + week)
                if size > TREND_DIRECTORY_THRESHOLD_MB * 1024 * 1024:
                    # print week, size, 'MB'
                    path_dirs_trend = path_weekly_trends + os.sep + week
                    dirs_trend = os.listdir(path_dirs_trend)
                    if 'Spreadsheets' in dirs_trend:
                        path_spreadsheets = path_dirs_trend + os.sep + 'Spreadsheets'
                        files = [f for f in os.listdir(path_spreadsheets) if os.path.isfile(os.path.join(path_spreadsheets, f))]
                        index_trends = [i for i, s in enumerate(files) if 'Trends' in s][0]
                        # print files[index_trends]

                        path_trends_csv = path_spreadsheets + os.sep + files[index_trends]
                        with open(path_trends_csv, 'rb') as csvfile:
                            # ignore the first 10 lines
                            for _ in xrange(10):
                                next(csvfile)

                            # create a DictReader to index the data in the CSV file
                            reader = csv.DictReader(csvfile)
                            for row in reader:
                                csv_date = 'Start Date and Time'

                                if csv_date in row:
                                    try:
                                        date = datetime.datetime.strptime(row[csv_date], '%Y/%m/%d %H:%M:%S')
                                    except ValueError:
                                        print 'wrong date format:', row[csv_date], path_trends_csv
                                        # date = datetime.datetime.strptime(row[csv_date], '%d/%m/%Y %H:%M')
                                    # date_tuple = (date.year, date.month, date.day, date.hour, date.minute)
                                    # time = datetime.time.strftime(row[csv_date], '%H %M %S')
                                    # print date
                                    if date < m['earliest_date']:
                                        m['earliest_date'] = date
                                    if date > m['latest_date']:
                                        m['latest_date'] = date

                                    # re-enable duplicate check (very time-consuming)
                                    # values = [row_existing['date'] for row_existing in table.where('(date == ' + str(calendar.timegm(date.timetuple())) + ')')]
                                    # if len(values) != 0:
                                    #     print 'date already exists'
                                    # else:
                                    data_row['date']  = calendar.timegm(date.timetuple())
                                    data_row['batch'] = batch_number
                                    data_row['flags'] = row['Flags']

                                    for item in CSV_labels:
                                        data_row[item] = get_data_from_CSV_label(CSV_labels[item], row)
                                    data_row.append()
            table.flush()

    # determine global date extremities for table, which may involve multiple batches
    if group._v_attrs.__contains__('earliest_date'):
        if m['earliest_date'] < datetime.datetime.utcfromtimestamp(group._v_attrs.earliest_date):
            group._v_attrs.earliest_date = calendar.timegm(m['earliest_date'].timetuple())
        if m['latest_date'] > datetime.datetime.utcfromtimestamp(group._v_attrs.latest_date):
            group._v_attrs.latest_date = calendar.timegm(m['latest_date'].timetuple())
    else:
        group._v_attrs.earliest_date = calendar.timegm(m['earliest_date'].timetuple())
        group._v_attrs.latest_date = calendar.timegm(m['latest_date'].timetuple())


def process_batch(path_to_data, filename='', monitoring_type='monitoring', harmonics_filename=''):
    batch_number = int(path_to_data[-1])

    all_monitors = []

    # find sub-directories at the root
    dirs = [d for d in os.listdir(path_to_data) if os.path.isdir(os.path.join(path_to_data, d))]

    # create a data structure for storing all data
    rings = [{'primary_name' : x.split(' - ')[-1], 'ring_ID': int(x.split(' ')[1]), 'path' : path_to_data + os.sep + x} for x in dirs]

    # remove rings which cannot be imported yet
    # for a in rings_to_ignore:
    #     rings = [r for r in rings if a not in r['primary_name']]

    for r in rings:
        # get directories, which should represent a PQube monitoring location
        monitors = os.listdir(r['path'])

        # ignore other directories which may exist
        monitors = [m for m in monitors if '11kV Feeder CB Data' not in m and 'IPSA Model' not in m]
        
        # add each monitoring location to the data structure
        for m in monitors:
            # check if exists from a previous batch before adding to list
            if len([1 for found in all_monitors if found['name'] == m]) == 0:
                all_monitors.append({
                    'primary_name': r['primary_name'],
                    'ring_ID': r['ring_ID'],
                    'name' : m,
                    'path' : r['path'] + os.sep + m,
                    # 'has_2012' : False, 'has_2013' : False, 'has_2014' : False,
                    'months' : [],
                    'earliest_date' : datetime.datetime(2099, 1, 1),
                    'latest_date' : datetime.datetime(2000, 1, 1)
                })

    print path_to_data, len(rings), 'rings,', len(all_monitors), 'monitors'

    for m in all_monitors:
        # open the data file in write mode (or append mode if exists)
        fullfilename = filename + '.h5'
        # if monitoring_type == 'harmonics':
        #     fullfilename = filename + '-' + harmonics_filename + '.h5'
        if os.path.isfile(fullfilename):
            h5file = open_file(fullfilename, mode='a')
            # print 'found file'
        else:
            h5file = open_file(fullfilename, mode='w', title='PQube monitoring data file')
            # print 'creating new file'

        monitor_tables = h5file.root
        table = None
        abbreviated_name = m['name'].replace(' ', '').replace('(', '').replace(')', '').replace('.', '')
        print abbreviated_name

        # check group/table exists; otherwise create new
        for node in monitor_tables:
            if node._v_title == m['name']:
                group = monitor_tables._f_get_child(abbreviated_name)
                table = node._f_get_child('readout')
                print 'appending to table'
                break
        if table == None:
            print 'creating new table'
            # create a new group under '/' (root)
            group = h5file.create_group('/', abbreviated_name, m['name'])
            # create a table in it
            if monitoring_type == 'harmonics':
                table = h5file.create_table(group, 'readout', Harmonics, 'Readout', filters=FILTER)
            elif monitoring_type == 'events':
                table = h5file.create_table(group, 'readout', Events, 'Readout', filters=FILTER)
            else:
                table = h5file.create_table(group, 'readout', Monitoring, 'Readout', filters=FILTER)

            group._v_attrs.ring_ID = m['ring_ID']
            group._v_attrs.primary_name = m['primary_name']
            group._v_attrs.primary_name_formatted = formatted_substation_name(m['primary_name'])
            group._v_attrs.secondary_name_formatted = formatted_substation_name(m['name'])
            group._v_attrs.secondary_ID = get_secondary_ID(m['name'])
            print m['name'], 'primary:', group._v_attrs.primary_name_formatted, 'secondary:', group._v_attrs.secondary_name_formatted, 'ID:', group._v_attrs.secondary_ID

        print 'indexing'
        if table.cols.date.index is not None:
            indexrows2 = table.cols.date.reindex()
        else:
            indexrows2 = table.cols.date.create_csindex()#optlevel=9, kind='full'

        # fill the table with data

        data_row = table.row
        monitor_dirs = os.listdir(m['path'])

        # # check for entries in 2012: no valid entries exist for any PQube
        # if '2012' in monitor_dirs:
        #     print 'starting 2012'
        #     import_year(m, group, table, data_row, '2012', batch_number, harmonics, filename)
        #     # print '2012:', get_size(start_path=m['path'] + os.sep + '2012')

        # find each day with valid data in 2013
        if '2013' in monitor_dirs:
            print 'starting 2013'
            import_year(m, group, table, data_row, '2013', batch_number, monitoring_type, filename)

        if '2014' in monitor_dirs:
            print 'starting 2014'
            import_year(m, group, table, data_row, '2014', batch_number, monitoring_type, filename)

        # update timestamps
        # if group.__contains__('earliest_date'):
        #     if m['earliest_date'] < datetime.datetime.utcfromtimestamp(group._v_attrs.earliest_date):
        #         group._v_attrs.earliest_date = calendar.timegm(m['earliest_date'].timetuple())
        #     if m['latest_date'] > datetime.datetime.utcfromtimestamp(group._v_attrs.latest_date):
        #         group._v_attrs.latest_date = calendar.timegm(m['latest_date'].timetuple())
        # else:
        #     group._v_attrs.earliest_date = calendar.timegm(m['earliest_date'].timetuple())
        #     group._v_attrs.latest_date = calendar.timegm(m['latest_date'].timetuple())

        if group._v_attrs.__contains__('earliest_date') == False:
            print 'dates missing'
            group._v_attrs.earliest_date = calendar.timegm(m['earliest_date'].timetuple())
            group._v_attrs.latest_date = calendar.timegm(m['latest_date'].timetuple())
        group._v_attrs.total_rows = table.nrows
        group._v_attrs.total_days = table.nrows / (12 * 24)

        print group._v_attrs.total_rows, 'rows from', m['earliest_date'], 'to', m['latest_date'], '(overall: ' + str(datetime.datetime.utcfromtimestamp(group._v_attrs.earliest_date)) + ' to ' + str(datetime.datetime.utcfromtimestamp(group._v_attrs.latest_date)) + ')'

        print 're-indexing'
        if table.cols.date.index is not None:
            indexrows2 = table.cols.date.reindex()
        else:
            indexrows2 = table.cols.date.create_csindex()#optlevel=9, kind='full'

        # close and flush the data file
        h5file.close()



# re-open, sort the table, and check for inconsistent timestamps
def resort_tables(filename):
    t0 = time.time()
    # open database file
    f = open_file(filename + '.h5', mode='r+')#, driver="H5FD_CORE")
    monitor_tables = f.root

    t1 = time.time()

    for node in monitor_tables:
        table = node._f_get_child('readout')
        table.copy(newname='readout_sorted', sortby=table.cols.date, propindexes=True, overwrite=True)

        table = node._f_get_child('readout')
        table.remove()

        table = node._f_get_child('readout_sorted')
        table.rename('readout')

        #table = table.readSorted(sortby=table.cols.date)

        # verify timestamps are sorted: are increasing in order
        values = [row['date'] for row in table]
        num = len(values)
        print node._v_title, 'rows:', num
        for i in range(0, num):
            # check for 'isolated' initial value
            if 'Harmonics' in filename and i == 0 and num > 1:
                if abs(values[i + 1] - values[i]) != 60 * 15:
                    print 'changing earliest_date', values[i + 1], values[i], table._v_parent._v_attrs.earliest_date
                    table._v_parent._v_attrs.earliest_date = values[i + 1]

            # list bounds check
            if i == num - 1:
                continue
            if values[i] > values[i + 1]:
                print '  inconsistent timestamp', i, datetime.datetime.utcfromtimestamp(values[i]), datetime.datetime.utcfromtimestamp(values[i + 1])

    t2 = time.time()

    print 'time to sort file:', t2 - t1, 'time to open table:', t1 - t0

    # close and flush the data file
    f.close()


# list of all monitors, from potentially from multiple batches
# all_monitors = []

# paths to directories containing PQube data batches
paths_to_batches = ['D:/Batch1', 'D:/Batch2', 'D:/Batch3']

for path in paths_to_batches:
    process_batch(path, MONITORING_DATA_FILENAME)
    for harm_filename in HARMONICS_DATA_FILENAMES:
        process_batch(path, harm_filename, monitoring_type='harmonics', harmonics_filename=harm_filename)
    process_batch(path, EVENTS_DATA_FILENAME, monitoring_type='events')


resort_tables(MONITORING_DATA_FILENAME)
for harm_filename in HARMONICS_DATA_FILENAMES:
    resort_tables(harm_filename)
resort_tables(EVENTS_DATA_FILENAME)