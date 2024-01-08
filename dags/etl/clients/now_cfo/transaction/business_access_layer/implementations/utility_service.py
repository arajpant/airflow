from abc import ABC, abstractmethod

class Implementation_Work(ABC):
    
    @abstractmethod
    def parse_json(self):
        """
        Abstract function for the parse json
        """
        pass
    
    @abstractmethod
    def dump_result_csv(self, json_dictionary, csv_dictionary):
        """
        Abstract function for the dumping the result csv
        

        Args:
            json_dictionary (json): json related object definition
            csv_dictionary (json): csv related object definition
        """
        pass