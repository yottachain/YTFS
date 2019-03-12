package opt

import (
	"io/ioutil"
	"testing"
)

func TestSaveConfig(t *testing.T) {
	file, err := ioutil.TempFile("", "sample-config.json")
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultOptions()
	SaveConfig(config, file.Name())
	t.Log("Save config file success", file.Name())
}
