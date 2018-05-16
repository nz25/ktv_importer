from enum import IntEnum

class DataTypeConstants(IntEnum):
    mtNone = 0
    mtLong = 1
    mtText = 2
    mtCategorical = 3
    mtObject = 4
    mtDate = 5
    mtDouble = 6
    mtBoolean = 7

class VariableUsageConstants(IntEnum):
    vtVariable = 0
    vtGrid = 1
    vtCompound = 2
    vtClass = 4
    vtArray = 8
    vtHelperField = 16
    vtSourceFile = 272
    vtCoding = 528
    vtOtherSpecify = 1040
    vtMultiplier = 2064
    vtFilter = 4096
    vtWeight = 8192

class wtMethod(IntEnum):
    wtFactors = 1
    wtTargets = 2
    wtRims = 3

class wtTotalType(IntEnum):
    wtTargetSum = 1
    wtUnweightedInput = 2
    wtTotalValue = 3