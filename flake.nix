{
  description = "oar-scheduler-redox";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs?ref=nixos-25.05";
    kapack = {
      url = "github:oar-team/nur-kapack/oar-redox";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, kapack }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        kapack-pkgs = kapack.packages.${system};


        pythonEnv = with pkgs.python3Packages; [
          simpy
          kapack-pkgs.oar
          #pkgs.maturin
        ];
      in
        {

          packages.oar-scheduler-redox =
            pkgs.python3Packages.buildPythonPackage {
              pname = "oar-scheduler-redox";
              version = "0.0.1";
              pyproject = true;
              src = ./.;
              nativeBuildInputs = with pkgs; [
                rustPlatform.cargoSetupHook
                rustPlatform.maturinBuildHook
              ];

              # find a better way to indicate where to operate
              configurePhase = ''cd oar-scheduler-redox'';

              cargoDeps = pkgs.rustPlatform.importCargoLock {
                lockFile = ./Cargo.lock;
              };
            };

          packages.oar-scheduler-meta-redox =
            pkgs.rustPlatform.buildRustPackage {
              pname = "oar-scheduler-meta-redox";
              version = "0.0.0";
              src = ./.;

              cargoBuildFlags = [ "--package" "oar-scheduler-meta" ];
              cargoInstallFlags = [ "--package" "oar-scheduler-meta" ];

              cargoDeps = pkgs.rustPlatform.importCargoLock {
                lockFile = ./Cargo.lock;
              };
              doCheck = false;
          };

          defaultPackage = self.packages.${system}.oar-scheduler-redox;

          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              # packages.default
              postgresql
              just
              pkg-config
              nixpkgs-fmt
              rustfmt
              rustc
              cargo
              pythonEnv
            ];
            shellHook = ''
              export OAR_PKG=${kapack-pkgs.oar}
            '';
          };
        }
    );
}
