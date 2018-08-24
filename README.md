# nifi-processor-CheckDiskSpace
A NiFi processor that monitors the space available and stops processing (enabling back pressure) when available space is
below the threshold.

The intended use case is a flow where you are storing data a local disk (i.e. using PutFile), but would rather have back
pressure than disks filling up.

