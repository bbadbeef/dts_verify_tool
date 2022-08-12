package compare

type toFinishingJob struct {
	assistJob
}

func (tfj *toFinishingJob) do() (bool, error) {
	tfj.setStatus(Finishing)
	return true, nil
}

func (tfj *toFinishingJob) name() string {
	return ""
}
