{
  description = "Errm... WS!, an erlang library for Websockets that's easy to use.";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {inherit system;};
    beamPackages = pkgs.beamPackages;

    errm-prod = beamPackages.buildRebar3 {
      name = "errm-WS";
      version = "0.1.0-prod";

      src = ./.;

      env = {
        REBAR_PROFILE = "prod";
      };
    };

    errm-debug = beamPackages.buildRebar3 {
      name = "errm-WS";
      version = "0.1.0-debug";

      src = ./.;

      env = {
        REBAR_PROFILE = "debug";
      };
    };
  in {
    packages.${system} = {
      errm-ws-prod = errm-prod;
      default = errm-prod;
      errm-ws-debug = errm-debug;
    };

    devShells.${system}.default = pkgs.mkShell {
      name = "errm-WS";

      buildInputs = with pkgs; [
        beamPackages.erlang
        beamPackages.rebar3
      ];

      packages = with pkgs; [
        erlang-language-platform
      ];
    };
  };
}
