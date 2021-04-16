module move_sectors

go 1.15

require (
	github.com/filecoin-project/lotus v1.6.0
	github.com/ipfs/go-log v1.0.5
	github.com/mitchellh/go-homedir v1.1.0
	github.com/urfave/cli/v2 v2.3.0
	gopkg.in/yaml.v2 v2.3.0
)

replace move_sectors => ./
