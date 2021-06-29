package common

const (
	//job saving directory
	JOB_SAVE_DIR = "/cron/jobs/"

	//job force kill directory
	JOB_KILLER_DIR = "/cron/killer/"

	//Save job event
	JOB_EVENT_SAVE = 1

	//Delete job event
	JOB_EVENT_DELETE = 2

	//Kill job event
	JOB_EVENT_KILL = 3

	//job lock directory
	JOB_LOCK_DIR = "/cron/lock/"

)
