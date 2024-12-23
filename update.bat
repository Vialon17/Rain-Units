@echo off
conda activate vi
echo 'Updating the tool_lib module...'
rmdir /S/Q rainv.egg-info
rmdir /S/Q build
pip install .
echo 'Have Finished Update.'
pause

