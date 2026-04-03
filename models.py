from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import Field
# enum is a special class where we define some fixed set of values
# here i have imported enum class to define enumerations , typing for List , Dictionary , and pydantic dor the data validation , type convversion and structuring the data
# field will provide additional metadata about the fields


# i have added open env core in requirements.txt file , and tried to import action , observation , and state . here is the 
# try - except , like if open env core is not installed it fallbacks to defining dummy classes
try:
    from openenv.core.env_server import Action, Observation, State
except ImportError:
    # Fallbacls for strict typing if openenv-core isn't installed yet
    from pydantic import BaseModel

    #although this is somehow not required as i have added open env core in requirements.txt file
    #but it is good to have it for the sake of completeness
    class Action(BaseModel): pass
    # here pass will help to bypass the class for not having fields
    class Observation(BaseModel): pass
    class State(BaseModel): pass


# now , here standardization strategy is a class i have inherits from str and enum  , it means that enum values are also strings
# this is used for the standardization of the data
class StandardizationStrategy(str, Enum):


    TO_DATETIME_ISO = "TO_DATETIME_ISO" #this will convert values to ISO datetime format 
    # so instead of passing "TO_DATETIME_ISO" as a string , we can pass this enum value
    LOWERCASE_STRIP = "LOWERCASE_STRIP" # this will convert values to lowercase and strip whitespace
    EXTRACT_NUMBERS = "EXTRACT_NUMBERS" # this will extract numbers from the values
    PHONE_E164 = "PHONE_E164" # this will convert values to E.164 format

class MissingStrategy(str, Enum):
    FILL_MEAN = "FILL_MEAN" # this will fill missing values with the mean of the column
    FILL_MODE = "FILL_MODE" # this will fill missing values with the mode of the column
    FILL_VALUE = "FILL_VALUE" # this will fill missing values with the specified value
    DROP_ROW = "DROP_ROW" # this will drop rows with missing values

class ConflictRule(str, Enum):
    PREFER_S1 = "PREFER_S1" # this will prefer the values from the first source
    PREFER_S2 = "PREFER_S2" # this will prefer the values from the second source
    COALESCE = "COALESCE" # this will coalesce the values from both sources
    
class DeduplicationStrategy(str, Enum):
    FUZZY_NAME_PHONE = "FUZZY_NAME_PHONE" # this will deduplicate the values based on the fuzzy name and phone
    EXACT_EMAIL = "EXACT_EMAIL" # this will deduplicate the values based on the exact email


#now here we defined some possible actions which can be possible in our crm pipeline 
class PipelineActionType(str, Enum):
    VIEW_SOURCE = "VIEW_SOURCE" # this will view the source
    PROFILE_SOURCE = "PROFILE_SOURCE" # this will profile the source
    STANDARDIZE_COLUMN = "STANDARDIZE_COLUMN" # this will standardize the column
    HANDLE_MISSING = "HANDLE_MISSING" # this will handle the missing values
    MERGE_SOURCES = "MERGE_SOURCES" # this will merge the sources
    DEDUPLICATE = "DEDUPLICATE" # this will deduplicate the values
    EXECUTE_SQL = "EXECUTE_SQL" # this will execute the sql query
    SUBMIT_PIPELINE = "SUBMIT_PIPELINE" # this will submit the pipeline

#now here we defined the action class which is inherited from Action class

# so my crmpipelineaction is like a container which decides exactly what action is needed to be perform on which datasets with what strategy
class CRMPipelineAction(Action):
    action_type: PipelineActionType = Field(..., description="The type of action to perform") #this will explain which action is of which type
    source: Optional[str] = Field(None, description="The primary dataset source to act upon") #this will explain which source is to be acted upon
    source2: Optional[str] = Field(None, description="Secondary source for merging") #this will explain which source is to be merged
    column: Optional[str] = Field(None, description="Column to mutate") #this will explain which column is to be mutated
    standardization_strategy: Optional[StandardizationStrategy] = None #this will explain which standardization strategy is to be used
    missing_strategy: Optional[MissingStrategy] = None #this will explain which missing strategy is to be used
    deduplication_strategy: Optional[DeduplicationStrategy] = None #this will explain which deduplication strategy is to be used
    fallback_value: Optional[str] = None #this will explain which fallback value is to be used
    join_key: Optional[str] = None #this will explain which join key is to be used
    conflict_rule: Optional[ConflictRule] = None #this will explain which conflict rule is to be used
    final_source: Optional[str] = Field(None, description="Dataset to submit as the final pipeline result") #this will explain which final source is to be used
    query: Optional[str] = Field(None, description="Raw SQL query for EXECUTE_SQL action") #this will explain which sql query is to be used
    output_table: Optional[str] = Field(None, description="Table name to write SQL output into")




#now here we defined the observation class which is inherited from Observation class
# this class represenet the response of the environemnt after the action is performed
class CRMPipelineObservation(Observation):
    done: bool # explains is the task is comleted?
    reward: Optional[float] # reward for the action performed
    current_task_objective: str # explains the current task objective
    schema_target: Dict[str, str] # explains the schema of the target dataset
    available_sources: List[str] # explains the available sources
    current_view: str  # Markdown table string, max 3 rows to save tokens
    data_quality_report: Optional[str] # Markdown string of quality report
    last_action_feedback: str #what feedback is recieved
    conflict_rules: Optional[Dict[str, str]] = Field(
        None,
        description="Per-column source priority rules for t3 (e.g. prefer_salesforce / prefer_web_leads)"
    )

    # this agent will observe and will decide what next action is needed to be taken



# this class define the current state of the current pipeline
class CRMPipelineState(State):
    episode_id: Optional[str] = None #pipeline's unique id
    step_count: int = 0 #number of steps taken
    task_id: str = "t1" #id of the current task
