import numpy as np


class DataUtilities:
    day_name_mapping = {
            "MONDAY": "Ponedeljak",
            "TUESDAY": "Utorak",
            "WEDNESDAY": "Sreda",
            "THURSDAY": "Četvrtak",
            "FRIDAY": "Petak",
            "SATURDAY": "Subota",
            "SUNDAY": "Nedelja"
        }
    
    @staticmethod
    def convert_to_native(obj):
        if isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: DataUtilities.convert_to_native(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [DataUtilities.convert_to_native(i) for i in obj]
        return obj
    
    @staticmethod
    def convert_day_name(day_name: str) -> str:
            eng_upper = day_name.upper()
            return DataUtilities.day_name_mapping.get(eng_upper, "Invalid day name")

