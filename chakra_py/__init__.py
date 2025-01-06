import os

from colorama import Fore, Style

from .client import Chakra

__version__ = "1.0.6"
__all__ = ["Chakra"]

BANNER = rf"""{Fore.GREEN}
 _____ _           _               ________   __
/  __ \ |         | |              | ___ \ \ / /
| /  \/ |__   __ _| | ___ __ __ _  | |_/ /\ V / 
| |   | '_ \ / _` | |/ / '__/ _` | |  __/  \ /  
| \__/\ | | | (_| |   <| | | (_| | | |     | |  
 \____/_| |_|\__,_|_|\_\_|  \__,_| \_|     \_/  
{Style.RESET_ALL}
                                   
Python SDK v{__version__}
"""

if not os.getenv("CHAKRA_QUIET"):
    print(BANNER.format(version=__version__))
