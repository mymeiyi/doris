#!/bin/bash
rm 0001-fix.patch
git add .
git commit -m "fix"
git format-patch -1
curl -T 0001-fix.patch  http://justtmp-bj-1308700295.cos.ap-beijing.myqcloud.com/meiyi/0001-fix.patch

